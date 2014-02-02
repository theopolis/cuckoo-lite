# Copyright (C) 2010-2014 Cuckoo Sandbox Developers.
# This file is part of Cuckoo Sandbox - http://www.cuckoosandbox.org
# See the file 'docs/LICENSE' for copying permission.

import os

from lib.cuckoo.common.abstracts import Report
from lib.cuckoo.common.exceptions import CuckooDependencyError
from lib.cuckoo.common.exceptions import CuckooReportError
from lib.cuckoo.common.objects import File

try:
    from pymongo.connection import Connection
    from pymongo.errors import ConnectionFailure
    from gridfs import GridFS
    from gridfs.errors import FileExists
    HAVE_MONGO = True
except ImportError:
    HAVE_MONGO = False

class MongoDB(Report):
    """Stores report in MongoDB."""

    def connect(self):
        """Connects to Mongo database, loads options and set connectors.
        @raise CuckooReportError: if unable to connect.
        """
        host = self.options.get("host", "127.0.0.1")
        port = self.options.get("port", 27017)

        try:
            self.conn = Connection(host, port)
            self.db = self.conn.cuckoo
            self.fs = GridFS(self.db)
        except TypeError:
            raise CuckooReportError("Mongo connection port must be integer")
        except ConnectionFailure:
            raise CuckooReportError("Cannot connect to MongoDB")

    def store_file(self, file_obj, filename=""):
        """Store a file in GridFS.
        @param file_obj: object to the file to store
        @param filename: name of the file to store
        @return: object id of the stored file
        """
        if not filename:
            filename = file_obj.get_name()

        existing = self.db.fs.files.find_one({"sha256": file_obj.get_sha256()})

        if existing:
            return existing["_id"]
        else:
            new = self.fs.new_file(filename=filename,
                                   sha256=file_obj.get_sha256())
            for chunk in file_obj.get_chunks():
                new.write(chunk)
            try:
                new.close()
            except FileExists:
                to_find = {"sha256": file_obj.get_sha256()}
                return self.db.fs.files.find_one(to_find)["_id"]
            else:
                return new._id

    def run(self, results):
        """Writes report.
        @param results: analysis results dictionary.
        @raise CuckooReportError: if fails to connect or write to MongoDB.
        """
        # We put the raise here and not at the import because it would
        # otherwise trigger even if the module is not enabled in the config.
        if not HAVE_MONGO:
            raise CuckooDependencyError("Unable to import pymongo "
                                        "(install with `pip install pymongo`)")

        self.connect()

        # Set an unique index on stored files, to avoid duplicates.
        # From pymongo docs:
        #  Returns the name of the created index if an index is actually
        #    created.
        #  Returns None if the index already exists.
        self.db.fs.files.ensure_index("sha256", unique=True,
                                      sparse=True, name="sha256_unique")

        # Create a copy of the dictionary. This is done in order to not modify
        # the original dictionary and possibly compromise the following
        # reporting modules.
        report = dict(results)

        # Store the sample in GridFS.
        if results["info"]["category"] == "file":
            sample = File(self.file_path)
            if sample.valid():
                fname = results["target"]["file"]["name"]
                sample_id = self.store_file(sample, filename=fname)
                report["target"] = {"file_id": sample_id}
                report["target"].update(results["target"])

        # Walk through the dropped files, store them in GridFS and update the
        # report with the ObjectIds.
        #new_dropped = []
        #for dropped in report["dropped"]:
        #    new_drop = dict(dropped)
        #    drop = File(dropped["path"])
        #    if drop.valid():
        #        dropped_id = self.store_file(drop, filename=dropped["name"])
        #        new_drop["object_id"] = dropped_id

        #    new_dropped.append(new_drop)

        #report["dropped"] = new_dropped

        # Store the report and retrieve its object id.
        self.db.analysis.save(report)
        self.conn.disconnect()
