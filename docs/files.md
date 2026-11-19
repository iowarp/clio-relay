# Files

The `files` verb works the same way on every site. This page is the contract:
an adapter that differs from it is a bug, and the adapter conformance kit
(`clio_relay.testing.conformance`) checks it.

## Paths

| Path | Means |
| --- | --- |
| `/abs/path` | The site's own path. On Tapis it is under the system's root directory. |
| `~/path` | The site user's home, on sites that have one (ssh, worker). |
| `rel/path` | Under relay's work directory on the site. `..` may not leave it. |
| empty | The work directory itself, for `list`. |
| `jobs/<operation id>` | A job's directory. Jobs run there, and their outputs are listed from it. |

A job's `OutputRef` paths are absolute, so they can be read back as they are.

## Operations

| Op | Arguments | Does | Refuses with |
| --- | --- | --- | --- |
| `list` | `path` | Lists a directory; a file lists as itself | `path_not_found` |
| `stat` | `path` | Describes one path: kind, size, modified time | `path_not_found` |
| `read` | `path`, `offset`, `limit` | Returns up to 4 MiB from `offset` | `path_not_found` |
| `write` | `path`, `content` or `content_base64`, `overwrite` | Writes a file of up to 4 MiB whole, making parent directories | `path_exists` without `overwrite` |
| `write` (chunk) | as above, plus `offset` and `final` | Adds one chunk of a larger upload (see below) | `invalid_arguments` with `resume_at` on a wrong offset |
| `delete` | `path`, `recursive` | Deletes a file, a symlink or a directory | `path_not_found`; `invalid_arguments` for a directory that is not empty without `recursive`, and for the work directory, the home directory or `/` |
| `stage` | `content` or `content_base64` | Puts bytes in the site's content store and returns their SHA-256, which a job input names | |
| `has` | `digest` | Says whether the content store holds a SHA-256 | |

`delete` removes a symlink itself and never follows it. A relative path that
leaves the work directory refuses with `invalid_arguments` on every op.

On a guarded site, `delete` and any write that replaces an existing file ask a
person first, on every adapter; a new file needs no approval. See
[guarded sites](guarded-sites.md#deleting-and-replacing-files).

## Large files

One call carries at most 4 MiB. Larger files move in chunks.

Upload: write the first chunk at `offset` 0 with `final=false`, each next
chunk at the offset where the last one ended, and the last chunk with
`final=true`. The chunks collect in `<path>.relay-part`; the last one moves it
to `path`. An adapter whose site cannot append keeps the part in relay's own
state and sends it up once, with the last chunk; `stat` on `<path>.relay-part`
still answers from it. `overwrite` is checked on the first chunk and again when the file
moves into place. A chunk with `final=false` answers with `resume_at`, the
offset of the next chunk.

Download: `read` at increasing offsets until a read returns fewer bytes than
asked for.

## Resuming after a dropped link

| Direction | Where to resume |
| --- | --- |
| Upload | `stat` on `<path>.relay-part` gives its size, which is the next offset. A chunk at any other offset refuses, and its `resume_at` names the right one. Starting again at offset 0 throws the part away. |
| Download | The client knows how many bytes it holds; it reads again from there. |

## Adapters

| Adapter | list, stat, read, whole write | delete | chunked write | Notes |
| --- | --- | --- | --- | --- |
| local, fixture | yes | yes | yes | The reference implementation. |
| any adapter built on `SiteFiles` | yes | yes | yes | `SiteFiles` applies these rules over six primitives; see [adding an adapter](adding-an-adapter.md#files). |
| worker | yes | yes | yes | The site side runs the local implementation. |
| tapis | yes | yes | yes | Tapis Files has no append, so chunks are staged locally and sent once with the final chunk. |
| ssh | yes | yes | yes | One remote script per op, so each op costs one ssh round trip. |
| globus | yes | yes | yes | Built on `SiteFiles`. Chunks are kept in `.relay-parts` in the local collection and moved with one transfer on the final chunk. |
| iri | yes | yes | yes, up to 5 MiB | Built on `SiteFiles`. The API takes at most 5 MiB per upload; a larger write refuses and points at globus. A recursive delete is one `rm`. |

A site that does not take an op yet refuses it with `verb_not_implemented`.
