''' Store files in a postgres database
'''
import json
import typing as t
import psycopg2
import logging
import traceback
logger = logging.getLogger(__name__)
from datetime import datetime
from textwrap import dedent
from ufs.spec import UFS, DescriptorFromAtomicMixin
from ufs.utils.pathlib import SafePurePosixPath_

class Postgres(DescriptorFromAtomicMixin, UFS):
  def __init__(self, database_url: str, database_schema: str = 'ufs'):
    super().__init__()
    assert database_url, 'database_url is required'
    self._database_url = database_url
    self._database_schema = database_schema
  
  @staticmethod
  def from_dict(*, database_url, database_schema):
    return Postgres(
      database_url=database_url,
      database_schema=database_schema,
    )

  def to_dict(self):
    return dict(super().to_dict(),
      database_url=self._database_url,
      database_schema=self._database_schema,
    )

  def start(self):
    self._con = psycopg2.connect(self._database_url)
    with self._con.cursor() as cur:
      try:
        cur.execute(dedent(f'''
          create schema if not exists {json.dumps(self._database_schema)};
          create table {json.dumps(self._database_schema)}."directory" (
            "path" varchar primary key
          );
          create table {json.dumps(self._database_schema)}."file" (
            "directory" varchar not null references {json.dumps(self._database_schema)}."directory" ("path"),
            "filename" varchar not null,
            "path" varchar generated always as ("directory" || '/' || "filename") stored,
            "content" bytea not null,
            "ctime" timestamp not null default now(),
            "atime" timestamp not null default now(),
            "mtime" timestamp not null default now(),
            primary key ("path")
          );
          insert into {json.dumps(self._database_schema)}."directory" ("path")
          values ('/');
        '''))
      except psycopg2.errors.Error as e:
        logger.warn(traceback.format_exc())
        self._con.rollback()
        pass
  
  def stop(self):
    self._con.close()
  
  def ls(self, path: SafePurePosixPath_):
    with self._con.cursor() as cur:
      cur.execute(dedent(f'''
        select array_agg(f."path")
        from {json.dumps(self._database_schema)}."directory" d
        inner join {json.dumps(self._database_schema)}."file" f on f."directory" = d."path"
        where d."path" = %(path)s;
      '''), dict(path=str(path)))
      listing, = cur.fetchone()
      if listing is None: raise FileNotFoundError(path)
      else: return listing

  def info(self, path: SafePurePosixPath_):
    with self._con.cursor() as cur:
      cur.execute(dedent(f'''
        select
          jsonb_build_object(
            'type', 'directory',
            'size', 0
          )
        from {json.dumps(self._database_schema)}."directory" d
        where d."path" = %(path)s
        union
        select
          jsonb_build_object(
            'type', 'file',
            'size', length(f.content),
            'ctime', f.ctime,
            'atime', f.atime,
            'mtime', f.mtime
          )
        from {json.dumps(self._database_schema)}."file" f
        where f."path" = %(path)s;
      '''), dict(path=str(path)))
      if cur.rowcount == 0:
        raise FileNotFoundError(path)
      info, = cur.fetchone()
      if 'ctime' in info: info['ctime'] = datetime.fromisoformat(info['ctime']).timestamp()
      if 'atime' in info: info['atime'] = datetime.fromisoformat(info['atime']).timestamp()
      if 'mtime' in info: info['mtime'] = datetime.fromisoformat(info['mtime']).timestamp()
      return info

  def unlink(self, path: SafePurePosixPath_):
    with self._con.cursor() as cur:
      cur.execute(dedent(f'''
        delete from {json.dumps(self._database_schema)}."file" f
        where f."path" = %(path)s;
      '''), dict(path=str(path)))
      if cur.rowcount == 0:
        raise FileNotFoundError(path)

  def mkdir(self, path: SafePurePosixPath_):
    with self._con.cursor() as cur:
      try:
        cur.execute(dedent(f'''
          insert into {json.dumps(self._database_schema)}."directory" ("path")
          select %(path)s
          where not exists (
            select *
            from {json.dumps(self._database_schema)}."file" f
            where f."path" = %(path)s
          );
        '''), dict(path=str(path)))
      except psycopg2.errors.UniqueViolation:
        self._con.rollback()
        raise FileExistsError(path)
      else:
        if cur.rowcount == 0:
          raise FileExistsError(path)

  def rmdir(self, path: SafePurePosixPath_):
    with self._con.cursor() as cur:
      try:
        cur.execute(dedent(f'''
          delete from {json.dumps(self._database_schema)}."directory" d
          where d."path" = %(path)s;
        '''), dict(path=str(path)))
      except psycopg2.errors.ForeignKeyViolation:
        self._con.rollback()
        raise RuntimeError('Directory not Empty')
      else:
        if cur.rowcount == 0:
          raise FileNotFoundError(path)

  def copy(self, src: SafePurePosixPath_, dst: SafePurePosixPath_):
    with self._con.cursor() as cur:
      try:
        cur.execute(dedent(f'''
          insert into {json.dumps(self._database_schema)}."file" ("directory", "filename", "content")
          select %(directory)s, %(filename)s, f."content"
          from {json.dumps(self._database_schema)}."file" f
          where f."path" = %(path)s;
        '''), dict(path=str(src), directory=str(dst.parent), filename=dst.name))
      except psycopg2.errors.ForeignKeyViolation:
        self._con.rollback()
        raise FileNotFoundError(dst.parent)
      except psycopg2.errors.UniqueViolation:
        self._con.rollback()
        raise FileExistsError(dst)
      else:
        if cur.rowcount == 0:
          raise FileNotFoundError(src)

  def cat(self, path: SafePurePosixPath_) -> t.Iterator[bytes]:
    with self._con.cursor() as cur:
      cur.execute(dedent(f'''
        select content
        from {json.dumps(self._database_schema)}."file" f
        where f."path" = %(path)s;
      '''), dict(path=str(path)))
      if cur.rowcount == 0:
        raise FileNotFoundError(path)
      content, = cur.fetchone()
      yield content

  def put(self, path: SafePurePosixPath_, data: t.Iterator[bytes], *, size_hint: int = None):
    with self._con.cursor() as cur:
      try:
        cur.execute(dedent(f'''
          insert into {json.dumps(self._database_schema)}."file" ("directory", "filename", "content")
          values (%(directory)s, %(filename)s, %(content)s)
          on conflict ("path")
          do update set "content" = excluded."content", "atime" = now(), "mtime" = now();
        '''), dict(directory=str(path.parent), filename=path.name, content=b''.join(data)))
      except psycopg2.errors.ForeignKeyViolation:
        self._con.rollback()
        raise FileNotFoundError(path.parent)
