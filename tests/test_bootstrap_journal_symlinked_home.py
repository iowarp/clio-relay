"""First-install bootstrap must work when an ANCESTOR directory is a symlink.

Found on ares (clio-relay#158): `/home` there is itself a symlink to
`/mnt/common/`, which is an ordinary HPC layout -- home directories live on the
big shared filesystem and `/home` is a pointer to it. The journal opens its
directory chain with O_NOFOLLOW from `/` downward, so the walk failed on the
very first component:

    FAILED at /home: NotADirectoryError errno=20
    is symlink? True -> /mnt/common/

surfacing as "bootstrap directory topology is unsafe". Every cluster with a
symlinked home was unbootstrappable.

The anti-swap property this walk exists for concerns the bootstrap-OWNED
subtree, not the site's stable layout above it. So the ancestor chain is
resolved once, up front, and the O_NOFOLLOW discipline is applied to the
resolved chain -- a symlink swapped in later still breaks the walk, and a
symlink AT the target itself is still refused.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from clio_relay import bootstrap_journal

pytestmark = pytest.mark.skipif(os.name == "nt", reason="descriptor-pinned traversal is POSIX")


def test_a_symlinked_ancestor_is_traversed(tmp_path: Path) -> None:
    """The ares shape: a symlink standing in for a real directory above us."""
    real_root = tmp_path / "shared"
    real_root.mkdir()
    (real_root / "user").mkdir()
    link_root = tmp_path / "home"
    link_root.symlink_to(real_root, target_is_directory=True)

    target = link_root / "user" / "state" / "component-wheels"

    with bootstrap_journal._open_absolute_directory(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        target, create=True
    ) as descriptor:
        assert isinstance(descriptor, int)

    assert (real_root / "user" / "state" / "component-wheels").is_dir()


def test_a_symlink_at_the_target_itself_is_still_refused(tmp_path: Path) -> None:
    """Resolving ancestors must not weaken the guard on the final component."""
    real_dir = tmp_path / "real"
    real_dir.mkdir()
    parent = tmp_path / "parent"
    parent.mkdir()
    impostor = parent / "component-wheels"
    impostor.symlink_to(real_dir, target_is_directory=True)

    opener = bootstrap_journal._open_absolute_directory(impostor)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    with pytest.raises(bootstrap_journal.BootstrapJournalError), opener:
        pass


def test_a_plain_chain_still_works(tmp_path: Path) -> None:
    target = tmp_path / "a" / "b"
    with bootstrap_journal._open_absolute_directory(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        target, create=True
    ) as descriptor:
        assert isinstance(descriptor, int)
    assert target.is_dir()
