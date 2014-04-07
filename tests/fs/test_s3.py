# Copyright 2009-2012 Yelp
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from StringIO import StringIO
import os
import gzip

try:
    import boto
    boto  # pyflakes
except ImportError:
    boto = None

from mrjob.fs.s3 import S3Filesystem

from tests.mockboto import MockS3Connection
from tests.mockboto import add_mock_s3_data
from tests.sandbox import SandboxedTestCase


class S3FSTestCase(SandboxedTestCase):

    def setUp(self):
        super(S3FSTestCase, self).setUp()
        self.sandbox_boto()
        self.addCleanup(self.unsandbox_boto)
        self.fs = S3Filesystem('key_id', 'secret', 'nowhere')

    def sandbox_boto(self):
        self.mock_s3_fs = {}

        def mock_boto_connect_s3(*args, **kwargs):
            kwargs['mock_s3_fs'] = self.mock_s3_fs
            return MockS3Connection(*args, **kwargs)

        self._real_boto_connect_s3 = boto.connect_s3
        boto.connect_s3 = mock_boto_connect_s3

        # copy the old environment just to be polite
        self._old_environ = os.environ.copy()

    def unsandbox_boto(self):
        boto.connect_s3 = self._real_boto_connect_s3

    def add_mock_s3_data(self, bucket, path, contents, time_modified=None):
        """Update self.mock_s3_fs with a map from bucket name
        to key name to data."""
        add_mock_s3_data(self.mock_s3_fs,
                         {bucket: {path: contents}},
                         time_modified)
        return 's3://%s/%s' % (bucket, path)

    def add_mock_s3_tree(self, bucket, path, files=None, time_modified=None):
        if files is None:
            files = ('f', 'g/a/b', 'g/a/a/b')
        test_files = [
            self.add_mock_s3_data(bucket, os.path.join(path, f), f)
            for f in sorted(files)
        ]
        self.assertEqual(
            sorted(self.fs.ls("s3://%s/%s/*" % (bucket, path.rstrip("/")))),
            test_files
        )
        return 's3://%s/%s' % (bucket, path)

    def test_cat_uncompressed(self):
        remote_path = self.add_mock_s3_data('walrus', 'data/foo', 'foo\nfoo\n')
        self.assertEqual(list(self.fs._cat_file(remote_path)), ['foo\n', 'foo\n'])

    def test_cat_gz(self):
        input = StringIO()
        input_gz = gzip.GzipFile(fileobj=input, mode="w")
        input_gz.write('foo\nfoo\n')
        input_gz.close()

        input.seek(0)
        remote_path = self.add_mock_s3_data('walrus', 'data/foo.gz', input.read())

        self.assertEqual(list(self.fs._cat_file(remote_path)),
                         ['foo\n', 'foo\n'])

    def test_ls_basic(self):
        remote_path = self.add_mock_s3_data('walrus', 'data/foo', 'foo\nfoo\n')

        self.assertEqual(list(self.fs.ls(remote_path)), [remote_path])
        self.assertEqual(list(self.fs.ls('s3://walrus/')), [remote_path])

    def test_ls_recurse(self):
        paths = [
            self.add_mock_s3_data('walrus', 'data/bar', ''),
            self.add_mock_s3_data('walrus', 'data/foo', 'foo\nfoo\n'),
        ]
        hidden = self.add_mock_s3_data('walrus', 'data/bar/baz', 'baz\nbaz\n')

        self.assertEqual(list(self.fs.ls('s3://walrus/')), paths)
        self.assertEqual(list(self.fs.ls('s3://walrus/*')), paths)

        # This is a fucky edge-case, but it's how hadoop fs behaves
        self.assertEqual(list(self.fs.ls('s3://walrus/data/bar')), [paths[0]])
        self.assertEqual(list(self.fs.ls('s3://walrus/data/bar/')), [hidden])

    def test_ls_glob(self):
        # A zero-byte directory created by some frameworks to represent a
        # "directory" within S3.
        data_path = self.add_mock_s3_data('walrus', 'data', '')
        paths = [
            # "Files"
            self.add_mock_s3_data('walrus', 'data/bar/baz', 'baz\nbaz\n'),
            self.add_mock_s3_data('walrus', 'data/foo', 'foo\nfoo\n'),
        ]

        self.assertEqual(list(self.fs.ls('s3://walrus/data')), [data_path])
        self.assertEqual(list(self.fs.ls('s3://walrus/data/')), paths)
        self.assertEqual(list(self.fs.ls('s3://walrus/*/baz')), [paths[0]])

    def test_ls_s3n(self):
        paths = [
            self.add_mock_s3_data('walrus', 'data/bar', 'abc123'),
            self.add_mock_s3_data('walrus', 'data/baz', '123abc')
        ]

        self.assertEqual(list(self.fs.ls('s3n://walrus/data/*')),
                         [ p.replace('s3://', 's3n://') for p in paths ])

    def test_du(self):
        paths = [
            self.add_mock_s3_data('walrus', 'data/foo', 'abcd'),
            self.add_mock_s3_data('walrus', 'data/bar/baz', 'defg'),
            self.add_mock_s3_data('walrus', 'data/empty', ''),
        ]
        self.assertEqual(self.fs.du('s3://walrus/'), 8)
        self.assertEqual(self.fs.du(paths[0]), 4)
        self.assertEqual(self.fs.du(paths[1]), 4)
        self.assertEqual(self.fs.du(paths[2]), 0)

    def test_path_exists_no(self):
        path = os.path.join('s3://walrus/data/foo')
        self.assertEqual(self.fs.path_exists(path), False)

    def test_path_exists_parent(self):
        path = self.add_mock_s3_data('walrus', 'data/foo', 'abcd')
        parent = os.path.dirname(path).rstrip("/")
        self.assertEqual(self.fs.path_exists(parent), True)
        self.assertEqual(self.fs.path_exists(parent + "/"), True)

    def test_path_exists_yes(self):
        path = self.add_mock_s3_data('walrus', 'data/foo', 'abcd')
        self.assertEqual(self.fs.path_exists(path), True)

    def test_rm(self):
        path = self.add_mock_s3_data('walrus', 'data/foo', 'abcd')
        self.assertEqual(self.fs.path_exists(path), True)

        self.fs.rm(path)
        self.assertEqual(self.fs.path_exists(path), False)

    def test_rm_tree_noslash_files(self):
        path = "icio/goodbye-1"
        s3_path = self.add_mock_s3_tree('walrus', path)
        self.assertEqual(s3_path, "s3://walrus/icio/goodbye-1")

        self.fs.rm(s3_path.rstrip("/"))

        # Check that the directory and its files have been removed
        # self.assertEqual(os.path.isdir(real_path), False)
        self.assertEqual(self.fs.path_exists(s3_path), False)
        self.assertEqual(list(self.fs.ls(s3_path)), [])

    def test_rm_tree_slash_files(self):
        path = "icio/goodbye-2"
        s3_path = self.add_mock_s3_tree('walrus', path)
        self.assertEqual(s3_path, "s3://walrus/icio/goodbye-2")

        self.fs.rm(s3_path.rstrip("/") + "/")

        # Check that the directory and its files have been removed
        # self.assertEqual(os.path.isdir(real_path), False)
        self.assertEqual(self.fs.path_exists(s3_path), False)
        self.assertEqual(list(self.fs.ls(s3_path)), [])

    def test_rm_tree_star_files(self):
        path = "icio/goodbye-3"
        s3_path = self.add_mock_s3_tree('walrus', path)
        self.assertEqual(s3_path, "s3://walrus/icio/goodbye-3")

        self.fs.rm(s3_path.rstrip("/") + "/*")

        # Check that the directory and its files have been removed
        self.assertEqual(self.fs.path_exists(s3_path), False)
        self.assertEqual(list(self.fs.ls(s3_path)), [])

    def test_write_str(self):
        # Ensure that the test bucket exists
        self.add_mock_s3_data('walrus', 'old-things', 'ensure bucket exists')

        path = "s3://walrus/new-things"
        content = 'some content!\n'
        self.fs.write(path, content)
        self.assertEqual("".join(self.fs.cat(path)), content)

    def test_write_file(self):
        # Ensure that the test bucket exists
        self.add_mock_s3_data('walrus', 'old-things', 'ensure bucket exists')

        path = "s3://walrus/other-new-things"
        content = StringIO('further content!\n')
        self.fs.write(path, content)
        self.assertEqual("".join(self.fs.cat(path)), content.getvalue())

    def test_copy_from_local(self):
        # Ensure that the test bucket exists
        self.add_mock_s3_data('walrus', 'old-things', 'ensure bucket exists')

        content = 'file filler\n'
        dst = 's3://walrus/new-things'
        src = self.makefile('local-source', content)

        self.fs.copy_from_local(dst, src)
        self.assertEqual("".join(self.fs.cat(dst)), content)

    def test_copy_from_local_override(self):
        self.add_mock_s3_data('walrus', 'exists', 'ensure bucket exists')
        src = self.makefile('local-source', 'content')
        self.assertRaises(OSError, self.fs.copy_from_local,
                          's3://walrus/exists', src)

    def test_overwrite(self):
        path = self.add_mock_s3_data('walrus', 'existing/file', 'herp')
        self.assertRaises(OSError, self.fs.write, path, 'derp')
