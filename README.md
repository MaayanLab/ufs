# Universal File System

Python Universal File System (UFS)

## Background
With the likes of PyFilesystem and Filesystem Spec (fsspec), why in the world do we need another?
- fsspec is not bad at all from a user standpoint, but it's exceedingly complex and difficult to extend with custom filesystems, especially those which support async.
- PyFilesystem is not bad either, but unfortunately, seems to be unmaintained and doesn't support asyncio.

The primary use for this is primarily for distributed execution of potentially untrusted code, so we additionally care about:
- operating on chroot jails
- fuse mounting

The goal here is to be as simple as possible while achieving the same things. Our goal is to be:
- a universal file system interface to use instead of the python built-in `os` or `pathlib` modules
- support for various cloud filesystems
- first class support for asyncio
- first class support for fuse mounting
- compatibility with existing efforts including fsspec & pyfilesystem

Generally, this interface strives to be simpler and easier to implement.
