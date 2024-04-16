(*
 * Copyright (c) 2015      Mindy Preston <mindy@somerandomidiot.com>
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
module Int63 = Optint.Int63

let test_kv = "test_directory"
let empty_file = Mirage_kv.Key.v "empty"
let content_file = Mirage_kv.Key.v "content"
let big_file = Mirage_kv.Key.v "big_file"
let directory = Mirage_kv.Key.v "a_directory"

module KV = Mirage_kv_unix

let lwt_run f () = Lwt_main.run (f ())
let assert_fail e = Alcotest.failf "%a" KV.pp_error e
let assert_write_fail e = Alcotest.failf "%a" KV.pp_write_error e

let get_ok = function
  | Ok x -> x
  | Error e -> Alcotest.failf "get_ok: %a" KV.pp_error e

let get_ok' = function
  | Ok x -> x
  | Error e -> Alcotest.failf "get_ok: %a" KV.pp_write_error e

let connect_present_dir () =
  let+ _ = KV.connect test_kv in
  ()

let append_timestamp s =
  let now = Ptime.v (Pclock.now_d_ps ()) in
  let str = Format.asprintf "%s-%a" s (Ptime.pp_rfc3339 ~space:false ()) now in
  Mirage_kv.Key.v str

let full_path dirname filename = Mirage_kv.Key.add dirname filename

let expect_error_connecting where () =
  Lwt.catch
    (fun () ->
      let* _ = KV.connect where in
      Lwt.fail_with "expected error")
    (fun _ -> Lwt.return_unit)

let size kv key =
  let+ n = KV.size kv key in
  let n = get_ok n in
  Int63.to_int n

let get kv ?offset key =
  let+ v =
    match offset with
    | None -> KV.get kv key
    | Some (offset, length) ->
        let offset = Int63.of_int offset in
        KV.get_partial kv key ~offset ~length
  in
  get_ok v

let set kv key ?offset v =
  let+ w =
    match offset with
    | None -> KV.set kv key v
    | Some offset ->
        let offset = Int63.of_int offset in
        KV.set_partial kv key ~offset v
  in
  get_ok' w

let connect_to_empty_string = expect_error_connecting ""
let connect_to_dev_null = expect_error_connecting "/dev/null"

let read_nonexistent_file file () =
  let key = Mirage_kv.Key.v file in
  let* kv = KV.connect test_kv in
  let+ v = KV.get kv key in
  match v with
  | Ok _ ->
      Alcotest.failf
        "read returned Ok when no file was expected. Please make sure there \
         isn't actually a file named %a"
        Mirage_kv.Key.pp key
  | Error (`Not_found _) -> ()
  | Error e ->
      Alcotest.failf
        "Unreasonable error response when trying to read a nonexistent file: %a"
        KV.pp_error e

let read_empty_file () =
  let* kv = KV.connect test_kv in
  let+ v = KV.get kv empty_file in
  match v with
  | Ok buf when String.length buf = 0 -> ()
  | Ok _ -> Alcotest.failf "reading an empty file returned some cstructs"
  | Error e ->
      Alcotest.failf "read failed for a present but empty file: %a" KV.pp_error
        e

let read_big_file () =
  let* kv = KV.connect test_kv in
  let* size = size kv big_file in
  let+ buf = get kv big_file in
  if String.length buf <> size then
    Alcotest.failf "read returned nothing for a large file"

let size_nonexistent_file () =
  let* kv = KV.connect test_kv in
  let filename = Mirage_kv.Key.v "^#$\000not a file!!!. &;" in
  let+ size = KV.size kv filename in
  match size with
  | Ok d -> Alcotest.failf "Got a size of %a for absent file" Int63.pp d
  | Error (`Not_found _) -> ()
  | Error e -> assert_fail e

let size_empty_file () =
  let* kv = KV.connect test_kv in
  let+ n = size kv empty_file in
  Alcotest.(check int) "size of an empty file" 0 n

let size_small_file () =
  let* kv = KV.connect test_kv in
  let+ n = size kv content_file in
  Alcotest.(check int) "size of a small file" 13 n

let size_a_directory () =
  let* kv = KV.connect test_kv in
  let+ size = KV.size kv directory in
  match size with
  | Error (`Value_expected _) -> ()
  | Error e -> assert_fail e
  | Ok n -> Alcotest.failf "got size %a on a directory" Int63.pp n

let size_big_file () =
  let* kv = KV.connect test_kv in
  let+ size = size kv big_file in
  Alcotest.(check int) __LOC__ 5000 size

let write_not_a_dir () =
  let dirname = append_timestamp "write_not_a_dir" in
  let subdir = "not there" in
  let content = "puppies" in
  let full_path = Mirage_kv.Key.(dirname / subdir / "file") in
  let* kv = KV.connect test_kv in
  let* () = set kv full_path content in
  let* exists = KV.exists kv full_path in
  match exists with
  | Error e ->
      Alcotest.failf "Exists on an existing file failed %a" KV.pp_error e
  | Ok None -> Alcotest.failf "Exists on an existing file returned None"
  | Ok (Some `Dictionary) ->
      Alcotest.failf "Exists on an existing file returned a dictionary"
  | Ok (Some `Value) ->
      let+ buf = get kv full_path in
      Alcotest.(check string) __LOC__ content buf

let write_zero_bytes () =
  let dirname = append_timestamp "mkdir_not_a_dir" in
  let subdir = "not there" in
  let full_path = Mirage_kv.Key.(dirname / subdir / "file") in
  let* kv = KV.connect test_kv in
  let* () = set kv full_path "" in
  (* make sure it's size 0 *)
  let+ n = KV.size kv full_path in
  match n with
  | Ok n -> Alcotest.(check int) __LOC__ 0 (Int63.to_int n)
  | Error e ->
      Alcotest.failf
        "write claimed to create a file that the kv then couldn't read: %a"
        KV.pp_error e

let write_contents_correct () =
  let dirname = append_timestamp "write_contents_correct" in
  let full_path = full_path dirname "short_phrase" in
  let phrase = "standing here on this frozen lake" in
  let* kv = KV.connect test_kv in
  let* () = set kv full_path phrase in
  let* v = get kv full_path in
  Alcotest.(check string) __LOC__ phrase v;
  let* v = get kv ~offset:(6, 10) full_path in
  Alcotest.(check string) __LOC__ "ng here on" v;
  let* () = set kv full_path ~offset:9 "foo" in
  let+ v = get kv ~offset:(6, 10) full_path in
  Alcotest.(check string) __LOC__ "ng fooe on" v

let write_overwrite_dir () =
  let dirname = append_timestamp "write_overwrite_dir" in
  let* kv = KV.connect test_kv in
  let subdir = Mirage_kv.Key.(dirname / "data") in
  let* () = set kv subdir "noooooo" in
  let+ w = KV.set kv dirname "noooooo" in
  match w with
  | Error (`Key_exists _) -> ()
  | Error e -> assert_write_fail e
  | Ok () ->
      Alcotest.failf
        "write overwrote an entire directory! That should not happen!"

let write_big_file () =
  let how_big = 4100 in
  let dirname = append_timestamp "write_big_file" in
  let full_path = full_path dirname "so many bytes!" in
  let zero_cstruct cs =
    let zero c = Cstruct.set_char c 0 '\000' in
    let i = Cstruct.iter (fun _ -> Some 1) zero cs in
    Cstruct.fold (fun b _ -> b) i cs
  in
  let first_page = zero_cstruct (Cstruct.create how_big) in
  Cstruct.set_char first_page 4097 'A';
  Cstruct.set_char first_page 4098 'B';
  Cstruct.set_char first_page 4099 'C';
  let* kv = KV.connect test_kv in
  (* TODO get rid of cstruct *)
  let* () = set kv full_path (Cstruct.to_string first_page) in
  let* sz = size kv full_path in
  let check_chars str a b c =
    Alcotest.(check char) __LOC__ 'A' (String.get str a);
    Alcotest.(check char) __LOC__ 'B' (String.get str b);
    Alcotest.(check char) __LOC__ 'C' (String.get str c)
  in
  Alcotest.(check int) __LOC__ how_big sz;
  let+ s = get kv full_path in
  if s = "" then Alcotest.failf "claimed a big file was empty on read"
  else check_chars s 4097 4098 4099

let populate num depth kv =
  let rec gen_d pref = function
    | 0 -> "foo"
    | x -> Filename.concat (pref ^ string_of_int x) (gen_d pref (pred x))
  in
  let rec gen_l acc = function
    | 0 -> acc
    | x -> gen_l (gen_d (string_of_int x) depth :: acc) (pred x)
  in
  (* populate a bit *)
  Lwt_list.iteri_s
    (fun i x ->
      let+ () =
        set kv (append_timestamp ("foo" ^ x ^ string_of_int i)) "test content"
      in
      ())
    (gen_l [] num)

let destroy () =
  let files =
    Mirage_kv.Key.to_string (append_timestamp ("/tmp/" ^ test_kv ^ "2"))
  in
  let* () = Lwt_unix.mkdir files 0o755 in
  let cleanup () = Lwt_unix.rmdir files in
  let* kv = KV.connect files in
  let* () = populate 10 4 kv in
  let* r = KV.remove kv Mirage_kv.Key.empty in
  match r with
  | Error _ ->
      let+ () = cleanup () in
      Alcotest.failf "create failed"
  | Ok () -> (
      let+ ls = KV.list kv Mirage_kv.Key.empty in
      match ls with
      | Ok [] -> ()
      | Ok _ -> Alcotest.failf "something exists after destroy"
      | Error e -> Alcotest.failf "error %a in listdir" KV.pp_error e)

let destroy_a_bit () =
  let files =
    Mirage_kv.Key.to_string (append_timestamp ("/tmp/" ^ test_kv ^ "3"))
  in
  let* () = Lwt_unix.mkdir files 0o755 in
  let cleanup () =
    let _ = Sys.command ("rm -rf " ^ files) in
    Lwt.return_unit
  in
  let* kv = KV.connect files in
  let* () = populate 10 4 kv in
  let* files = KV.list kv Mirage_kv.Key.empty in
  let files =
    match files with
    | Ok files -> List.length files
    | Error _ -> Alcotest.failf "error in list"
  in
  let* w = KV.set kv (Mirage_kv.Key.v "barf") "dummy content" in
  let* () =
    match w with
    | Error _ ->
        let+ () = cleanup () in
        Alcotest.failf "create failed"
    | Ok () -> Lwt.return ()
  in
  let* r = KV.remove kv (Mirage_kv.Key.v "barf") in
  let* () =
    match r with
    | Error _ ->
        let+ () = cleanup () in
        Alcotest.failf "destroy failed"
    | Ok () -> Lwt.return ()
  in
  let* xs = KV.list kv Mirage_kv.Key.empty in
  match xs with
  | Ok xs when List.length xs = files -> cleanup ()
  | Ok _ ->
      Alcotest.failf
        "something wrong in destroy: destroy  followed by create is not well \
         behaving"
  | Error _ -> Alcotest.failf "error in listdir"

let () =
  let connect =
    [
      ("connect_to_empty_string", `Quick, lwt_run connect_to_empty_string);
      ("connect_to_dev_null", `Quick, lwt_run connect_to_dev_null);
      ("connect_present_dir", `Quick, lwt_run connect_present_dir);
    ]
  in
  let read =
    [
      ( "read_nonexistent_file_from_root",
        `Quick,
        lwt_run (read_nonexistent_file "^$@thing_that_isn't_in root!!!.space")
      );
      ( "read_nonexistent_file_from_dir",
        `Quick,
        lwt_run
          (read_nonexistent_file
             "not a *dir*?!?/thing_that_isn't_in root!!!.space") );
      ("read_empty_file", `Quick, lwt_run read_empty_file);
      ("read_big_file", `Quick, lwt_run read_big_file);
    ]
  in
  let destroy =
    [
      ("destroy_file", `Quick, lwt_run destroy);
      ("create_destroy_file", `Quick, lwt_run destroy_a_bit);
    ]
  in
  let size =
    [
      ("size_nonexistent_file", `Quick, lwt_run size_nonexistent_file);
      ("size_empty_file", `Quick, lwt_run size_empty_file);
      ("size_small_file", `Quick, lwt_run size_small_file);
      ("size_a_directory", `Quick, lwt_run size_a_directory);
      ("size_big_file", `Quick, lwt_run size_big_file);
    ]
  in
  let listdir = [] in
  let write =
    [
      ("write_not_a_dir", `Quick, lwt_run write_not_a_dir);
      ("write_zero_bytes", `Quick, lwt_run write_zero_bytes);
      ("write_contents_correct", `Quick, lwt_run write_contents_correct);
      ("write_overwrite_dir", `Quick, lwt_run write_overwrite_dir);
      ("write_big_file", `Quick, lwt_run write_big_file);
    ]
  in
  Alcotest.run "KV"
    [
      ("connect", connect);
      ("read", read);
      ("size", size);
      ("destroy", destroy);
      ("listdir", listdir);
      ("write", write);
    ]
