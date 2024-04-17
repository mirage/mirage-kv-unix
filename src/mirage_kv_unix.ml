(*
 * Copyright (c) 2013-2014 Anil Madhavapeddy <anil@recoil.org>
 * Copyright (c) 2014      Thomas Gazagnaire <thomas@gazagnaire.org>
 * Copyright (c) 2014      Hannes Mehnert <hannes@mehnert.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

open Lwt.Syntax
open Optint

let ( let*? ) = Lwt_result.bind
let ( let+? ) x f = Lwt_result.map f x

type key = Mirage_kv.Key.t
type t = { base : string }
type error = [ Mirage_kv.error | `Storage_error of Mirage_kv.Key.t * string ]

exception Internal_error of error

let internal_error = function
  | Error e -> Lwt.fail (Internal_error e)
  | _ -> assert false

let storage_error key fmt =
  Format.ksprintf (fun str -> Error (`Storage_error (key, str))) fmt

let err_read key size = storage_error key "could not read %d bytes" size
let err_file_too_large key = storage_error key "file too large to process"
let err_not_a_file key = storage_error key "not a regular file"
let err_unix key e = storage_error key "%s" (Unix.error_message e)
let err_mtime key = storage_error key "mtime parsing failed"
let err_not_found key = Error (`Not_found key)
let err_dict_expected key = Error (`Dictionary_expected key)
let err_no_space = Error `No_space
let err_value_expected key = Error (`Value_expected key)

let pp_error ppf = function
  | #Mirage_kv.error as err -> Mirage_kv.pp_error ppf err
  | `Storage_error (key, msg) ->
      Format.fprintf ppf "storage error for %a: %s" Mirage_kv.Key.pp key msg

type write_error =
  [ Mirage_kv.write_error
  | `Storage_error of Mirage_kv.Key.t * string
  | `Key_exists of Mirage_kv.Key.t ]

let pp_write_error ppf = function
  | #Mirage_kv.write_error as err -> Mirage_kv.pp_write_error ppf err
  | `Key_exists key ->
      Format.fprintf ppf "key %a already exists and is a dictionary"
        Mirage_kv.Key.pp key
  | `Storage_error (key, msg) ->
      Format.fprintf ppf "storage error for %a: %s" Mirage_kv.Key.pp key msg

let split_string delimiter name =
  let len = String.length name in
  let rec doit off acc =
    let open String in
    let idx = try index_from name off delimiter with _ -> len in
    let fst = sub name off (idx - off) in
    let idx' = idx + 1 in
    if idx' <= len then doit idx' (fst :: acc) else fst :: acc
  in
  List.rev (doit 0 [])

let resolve_filename base key =
  let filename = Mirage_kv.Key.to_string key in
  let parts = split_string '/' filename in
  let ret =
    if List.exists (fun s -> s = "." || s = "..") parts then err_not_found key
    else Ok (Filename.concat base filename)
  in
  Lwt.return ret

let get_aux { base } ?offset ?length key =
  let*? path = resolve_filename base key in
  let size stat =
    match length with
    | Some n -> Lwt.return n
    | None ->
        let size64 = stat.Lwt_unix.LargeFile.st_size in
        if size64 > Int64.of_int Sys.max_string_length then
          internal_error (err_file_too_large key)
        else Lwt.return (Int64.to_int size64)
  in
  let lseek fd =
    match offset with
    | None -> Lwt.return ()
    | Some n ->
        let+ _ = Lwt_unix.LargeFile.lseek fd (Int63.to_int64 n) SEEK_SET in
        ()
  in
  Lwt.catch
    (fun () ->
      let* fd = Lwt_unix.openfile path [ Lwt_unix.O_RDONLY ] 0 in
      Lwt.finalize
        (fun () ->
          let* stat = Lwt_unix.LargeFile.fstat fd in
          if stat.Lwt_unix.LargeFile.st_kind = Lwt_unix.S_REG then
            let* () = lseek fd in
            let* size = size stat in
            let buffer = Bytes.create size in
            let+ read_bytes = Lwt_unix.read fd buffer 0 size in
            if read_bytes = size then Ok (Bytes.unsafe_to_string buffer)
            else err_read key size
          else Lwt.return (err_value_expected key))
        (fun () -> Lwt_unix.close fd))
    (function
      | Internal_error e -> Lwt.return (Error e)
      | Unix.Unix_error (ENOENT, _, _) -> Lwt.return (err_not_found key)
      | Unix.Unix_error (e, _, _) -> Lwt.return (err_unix key e)
      | e -> Lwt.reraise e)

let get_partial t key ~offset ~length = get_aux t key ~offset ~length
let get t key = get_aux t key
let disconnect _ = Lwt.return ()

(* all mkdirs are mkdir -p *)
let rec create_directory t key =
  let*? path = resolve_filename t.base key in
  let check_type path =
    let+ stat = Lwt_unix.LargeFile.stat path in
    match stat.Lwt_unix.LargeFile.st_kind with
    | Lwt_unix.S_DIR -> Ok ()
    | _ -> Error (`Dictionary_expected key)
  in
  if Sys.file_exists path then check_type path
  else
    let*? () = create_directory t (Mirage_kv.Key.parent key) in
    Lwt.catch
      (fun () ->
        let+ () = Lwt_unix.mkdir path 0o755 in
        Ok ())
      (function
        | Unix.Unix_error (e, _, _) -> Lwt.return (err_unix key e)
        | e -> Lwt.reraise e)

let open_file t key flags =
  let*? path = resolve_filename t.base key in
  let*? () = create_directory t (Mirage_kv.Key.parent key) in
  Lwt.catch
    (fun () ->
      let+ fd = Lwt_unix.openfile path flags 0o644 in
      Ok fd)
    (function
      | Unix.Unix_error (ENOSPC, _, _) -> Lwt.return err_no_space
      | Unix.Unix_error (e, _, _) -> Lwt.return (err_unix key e)
      | e -> Lwt.reraise e)

let file_or_directory { base } key =
  let*? path = resolve_filename base key in
  let+ stat = Lwt_unix.LargeFile.stat path in
  match stat.Lwt_unix.LargeFile.st_kind with
  | Lwt_unix.S_DIR -> Ok `Dictionary
  | Lwt_unix.S_REG -> Ok `Value
  | _ -> err_not_a_file key

(* TODO test this *)
let exists t key =
  Lwt.catch
    (fun () ->
      let+? x = file_or_directory t key in
      Some x)
    (function
      | Unix.Unix_error (ENOENT, _, _) -> Lwt.return (Ok None)
      | Unix.Unix_error (e, _, _) -> Lwt.return (err_unix key e)
      | e -> Lwt.reraise e)

let last_modified { base } key =
  let*? path = resolve_filename base key in
  Lwt.catch
    (fun () ->
      let+ stat = Lwt_unix.LargeFile.stat path in
      let mtime = stat.Lwt_unix.LargeFile.st_mtime in
      match Ptime.of_float_s mtime with
      | None -> err_mtime key
      | Some ts -> Ok ts)
    (function
      | Unix.Unix_error (e, _, _) -> Lwt.return (err_unix key e)
      | e -> Lwt.reraise e)

let size { base } key =
  let*? path = resolve_filename base key in
  Lwt.catch
    (fun () ->
      let+ stat = Lwt_unix.LargeFile.stat path in
      if stat.Lwt_unix.LargeFile.st_kind = Lwt_unix.S_REG then
        let size = stat.Lwt_unix.LargeFile.st_size in
        let size = Int63.of_int64 size in
        Ok size
      else err_value_expected key)
    (function
      | Unix.Unix_error (ENOENT, _, _) -> Lwt.return (err_not_found key)
      | Unix.Unix_error (e, _, _) -> Lwt.return (err_unix key e)
      | e -> Lwt.reraise e)

let connect id =
  try
    if Sys.is_directory id then Lwt.return { base = id }
    else failwith ("Not a directory " ^ id)
  with Sys_error _ -> failwith ("Not an entity " ^ id)

let list t key =
  let*? path = resolve_filename t.base key in
  Lwt.catch
    (fun () ->
      let s = Lwt_unix.files_of_directory path in
      let s = Lwt_stream.filter (fun s -> s <> "." && s <> "..") s in
      let* l = Lwt_stream.to_list s in
      Lwt_list.fold_left_s
        (fun result filename ->
          let*? files = Lwt.return result in
          let path = Mirage_kv.Key.add key filename in
          let+? kind = file_or_directory t path in
          (path, kind) :: files)
        (Ok []) l)
    (function
      | Unix.Unix_error (ENOENT, _, _) -> Lwt.return (err_not_found key)
      | Unix.Unix_error (ENOTDIR, _, _) -> Lwt.return (err_dict_expected key)
      | Unix.Unix_error (e, _, _) -> Lwt.return (err_unix key e)
      | e -> Lwt.reraise e)

let digest t key =
  let*? path = resolve_filename t.base key in
  Lwt.catch
    (fun () ->
      let*? v = file_or_directory t key in
      match v with
      | `Value -> Lwt.return (Ok (Digest.file path))
      | `Dictionary -> Lwt.return (err_value_expected key))
    (function
      | Unix.Unix_error (ENOENT, _, _) -> Lwt.return (err_not_found key)
      | Unix.Unix_error (e, _, _) -> Lwt.return (err_unix key e)
      | e -> Lwt.reraise e)

let rec remove t key =
  let*? path = resolve_filename t.base key in
  Lwt.catch
    (fun () ->
      let*? file = file_or_directory t key in
      match file with
      | `Value ->
          let+ () = Lwt_unix.unlink path in
          Ok ()
      | `Dictionary ->
          let*? files = list t key in
          let*? () =
            Lwt_list.fold_left_s
              (fun result (key, _) ->
                match result with
                | Error e -> Lwt.return (Error e)
                | Ok () -> remove t key)
              (Ok ()) files
          in
          if not Mirage_kv.Key.(equal empty key) then
            let+ () = Lwt_unix.rmdir path in
            Ok ()
          else Lwt.return (Ok ()))
    (function
      | Unix.Unix_error (ENOENT, _, _) -> Lwt.return (err_not_found key)
      | e -> Lwt.reraise e)

let set_aux t key ?offset value =
  let lseek fd =
    match offset with
    | None -> Lwt.return ()
    | Some offset ->
        let+ _ =
          Lwt_unix.LargeFile.lseek fd (Optint.Int63.to_int64 offset) SEEK_SET
        in
        ()
  in
  let* exists = exists t key in
  match exists with
  | Ok (Some `Dictionary) when offset <> None ->
      (* We are in the [set_partial] case *)
      Lwt.return (err_value_expected key)
  | _ -> (
      let* ret =
        match offset with
        | None ->
            (* [set] always overwite the previous bindings, even if it
               is a directory. *)
            remove t key
        | Some _ -> Lwt.return (Ok ())
      in
      match ret with
      | (Error (`Dictionary_expected _) | Error (`Storage_error _)) as e ->
          Lwt.return e
      | Ok () | Error (`Not_found _) ->
          Lwt.catch
            (fun () ->
              let*? fd =
                open_file t key Lwt_unix.[ O_WRONLY; O_NONBLOCK; O_CREAT ]
              in
              Lwt.finalize
                (fun () ->
                  let* () = lseek fd in
                  let buf = Bytes.unsafe_of_string value in
                  let rec write_once off len =
                    if len = 0 then Lwt.return ()
                    else
                      let* n_written = Lwt_unix.write fd buf off len in
                      if n_written = len + off then Lwt.return ()
                      else write_once (off + n_written) (len - n_written)
                  in
                  let+ () = write_once 0 (String.length value) in
                  Ok ())
                (fun () -> Lwt_unix.close fd))
            (function
              | Unix.Unix_error (ENOSPC, _, _) -> Lwt.return err_no_space
              | Unix.Unix_error (e, _, _) -> Lwt.return (err_unix key e)
              | e -> Lwt.reraise e))

let set_partial t key ~offset value = set_aux t key ~offset value
let set t key value = set_aux t key value

let allocate t key ?last_modified length =
  let len = Int63.to_int length in
  let value = String.make len '\000' in
  let set_last_modified () =
    match last_modified with
    | None -> Lwt.return (Ok ())
    | Some ts ->
        let*? path = resolve_filename t.base key in
        let date = Ptime.to_float_s ts in
        let+ () = Lwt_unix.utimes path date date in
        Ok ()
  in
  let*? () = set t key value in
  set_last_modified ()

let rename t ~source ~dest =
  let*? source_path = resolve_filename t.base source in
  let*? dest_path = resolve_filename t.base dest in
  Lwt.catch
    (fun () ->
      let+ () = Lwt_unix.rename source_path dest_path in
      Ok ())
    (function
      | Unix.Unix_error (e, _, _) -> Lwt.return (err_unix source e)
      | e -> Lwt.reraise e)
