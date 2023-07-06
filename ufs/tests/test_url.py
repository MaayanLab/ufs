def test_url_singlefile_mount():
  from ufs.access.url import ufs_file_from_url
  from ufs.access.mount import mount
  import logging; logging.getLogger(__name__).debug(__file__)
  ufs, filename = ufs_file_from_url(f"file://{__file__}")
  with mount(ufs, readonly=True) as mnt:
    assert [p.name for p in mnt.iterdir()] == ['test_url.py']
    assert (mnt/filename).stat().st_size > 0
