# Copyright (C) 2010-2014 Cuckoo Sandbox Developers.
# This file is part of Cuckoo Sandbox - http://www.cuckoosandbox.org
# See the file 'docs/LICENSE' for copying permission.

import os
import json
import logging
from datetime import datetime

from lib.cuckoo.common.config import Config
from lib.cuckoo.common.constants import CUCKOO_ROOT
from lib.cuckoo.common.exceptions import CuckooDatabaseError
from lib.cuckoo.common.exceptions import CuckooOperationalError
from lib.cuckoo.common.exceptions import CuckooDependencyError
from lib.cuckoo.common.objects import File, URL
from lib.cuckoo.common.utils import create_folder, Singleton

try:
    from sqlalchemy import create_engine, Column
    from sqlalchemy import Integer, String, Boolean, DateTime, Enum
    from sqlalchemy import ForeignKey, Text, Index, Table
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.exc import SQLAlchemyError, IntegrityError
    from sqlalchemy.orm import sessionmaker, relationship, joinedload, backref
    from sqlalchemy.pool import NullPool
    Base = declarative_base()
except ImportError:
    raise CuckooDependencyError("Unable to import sqlalchemy "
                                "(install with `pip install sqlalchemy`)")

log = logging.getLogger(__name__)

TASK_PENDING = "pending"
TASK_RUNNING = "running"
TASK_COMPLETED = "completed"
TASK_RECOVERED = "recovered"
TASK_REPORTED = "reported"
TASK_FAILED_ANALYSIS = "failed_analysis"
TASK_FAILED_PROCESSING = "failed_processing"

# Secondary table used in association Task - Tag.
tasks_tags = Table("tasks_tags", Base.metadata,
    Column("task_id", Integer, ForeignKey("tasks.id")),
    Column("tag_id", Integer, ForeignKey("tags.id"))
)

class Tag(Base):
    """Tag describing anything you want."""
    __tablename__ = "tags"

    id = Column(Integer(), primary_key=True)
    name = Column(String(255), nullable=False, unique=True)

    def __repr__(self):
        return "<Tag('{0}','{1}')>".format(self.id, self.name)

    def __init__(self,
                 name):
        self.name = name


class Sample(Base):
    """Submitted files details."""
    __tablename__ = "samples"

    id = Column(Integer(), primary_key=True)
    file_size = Column(Integer(), nullable=False)
    file_type = Column(String(255), nullable=False)
    md5 = Column(String(32), nullable=False)
    crc32 = Column(String(8), nullable=False)
    sha1 = Column(String(40), nullable=False)
    sha256 = Column(String(64), nullable=False)
    sha512 = Column(String(128), nullable=False)
    ssdeep = Column(String(255), nullable=True)
    __table_args__ = (Index("hash_index",
                            "md5",
                            "crc32",
                            "sha1",
                            "sha256",
                            "sha512",
                            unique=True), )

    def __repr__(self):
        return "<Sample('{0}','{1}')>".format(self.id, self.sha256)

    def to_dict(self):
        """Converts object to dict.
        @return: dict
        """
        d = {}
        for column in self.__table__.columns:
            value = getattr(self, column.name)
            d[column.name] = value
        return d

    def to_json(self):
        """Converts object to JSON.
        @return: JSON data
        """
        return json.dumps(self.to_dict())

    def __init__(self,
                 md5,
                 crc32,
                 sha1,
                 sha256,
                 sha512,
                 file_size,
                 file_type=None,
                 ssdeep=None):
        self.md5 = md5
        self.sha1 = sha1
        self.crc32 = crc32
        self.sha256 = sha256
        self.sha512 = sha512
        self.file_size = file_size
        if file_type:
            self.file_type = file_type
        if ssdeep:
            self.ssdeep = ssdeep

class Error(Base):
    """Analysis errors."""
    __tablename__ = "errors"

    id = Column(Integer(), primary_key=True)
    message = Column(String(255), nullable=False)
    task_id = Column(Integer, ForeignKey("tasks.id"), nullable=False)

    def to_dict(self):
        """Converts object to dict.
        @return: dict
        """
        d = {}
        for column in self.__table__.columns:
            value = getattr(self, column.name)
            d[column.name] = value
        return d

    def to_json(self):
        """Converts object to JSON.
        @return: JSON data
        """
        return json.dumps(self.to_dict())

    def __init__(self, message, task_id):
        self.message = message
        self.task_id = task_id

    def __repr__(self):
        return "<Error('{0}','{1}','{2}')>".format(self.id, self.message, self.task_id)

class Task(Base):
    """Analysis task queue."""
    __tablename__ = "tasks"

    id = Column(Integer(), primary_key=True)
    target = Column(Text(), nullable=False)
    category = Column(String(255), nullable=False)
    timeout = Column(Integer(), server_default="0", nullable=False)
    priority = Column(Integer(), server_default="1", nullable=False)
    custom = Column(String(255), nullable=True)
    package = Column(String(255), nullable=True)
    tags = relationship("Tag", secondary=tasks_tags, cascade="all, delete",
                        single_parent=True, backref=backref("task", cascade="all"),
                        lazy="subquery")
    options = Column(String(255), nullable=True)
    platform = Column(String(255), nullable=True)
    memory = Column(Boolean, nullable=False, default=False)
    enforce_timeout = Column(Boolean, nullable=False, default=False)
    clock = Column(DateTime(timezone=False),
                   default=datetime.now,
                   nullable=False)
    added_on = Column(DateTime(timezone=False),
                      default=datetime.now,
                      nullable=False)
    started_on = Column(DateTime(timezone=False), nullable=True)
    completed_on = Column(DateTime(timezone=False), nullable=True)
    status = Column(Enum(TASK_PENDING,
                         TASK_RUNNING,
                         TASK_COMPLETED,
                         TASK_REPORTED,
                         TASK_RECOVERED,
                         name="status_type"),
                         server_default=TASK_PENDING,
                         nullable=False)
    sample_id = Column(Integer, ForeignKey("samples.id"), nullable=True)
    sample = relationship("Sample", backref="tasks")
    errors = relationship("Error", backref="tasks", cascade="save-update, delete")

    def to_dict(self):
        """Converts object to dict.
        @return: dict
        """
        d = {}
        for column in self.__table__.columns:
            value = getattr(self, column.name)
            if isinstance(value, datetime):
                d[column.name] = value.strftime("%Y-%m-%d %H:%M:%S")
            else:
                d[column.name] = value

        # Tags are a relation so no column to iterate.
        d["tags"] = [tag.name for tag in self.tags]

        return d

    def to_json(self):
        """Converts object to JSON.
        @return: JSON data
        """
        return json.dumps(self.to_dict())

    def __init__(self, target=None):
        self.target = target

    def __repr__(self):
        return "<Task('{0}','{1}')>".format(self.id, self.target)

class Database(object):
    """Analysis queue database.

    This class handles the creation of the database user for internal queue
    management. It also provides some functions for interacting with it.
    """
    __metaclass__ = Singleton

    def __init__(self, dsn=None):
        """@param dsn: database connection string."""
        cfg = Config()

        if dsn:
            self.engine = create_engine(dsn, poolclass=NullPool)
        elif cfg.database.connection:
            self.engine = create_engine(cfg.database.connection, poolclass=NullPool)
        else:
            db_file = os.path.join(CUCKOO_ROOT, "db", "cuckoo.db")
            if not os.path.exists(db_file):
                db_dir = os.path.dirname(db_file)
                if not os.path.exists(db_dir):
                    try:
                        create_folder(folder=db_dir)
                    except CuckooOperationalError as e:
                        raise CuckooDatabaseError("Unable to create database directory: {0}".format(e))

            self.engine = create_engine("sqlite:///{0}".format(db_file), poolclass=NullPool)

        # Disable SQL logging. Turn it on for debugging.
        self.engine.echo = False
        # Connection timeout.
        if cfg.database.timeout:
            self.engine.pool_timeout = cfg.database.timeout
        else:
            self.engine.pool_timeout = 60
        # Create schema.
        try:
            Base.metadata.create_all(self.engine)
        except SQLAlchemyError as e:
            raise CuckooDatabaseError("Unable to create or connect to database: {0}".format(e))

        # Get db session.
        self.Session = sessionmaker(bind=self.engine)

    def __del__(self):
        """Disconnects pool."""
        self.engine.dispose()

    def _get_or_create(self, session, model, **kwargs):
        """Get an ORM instance or create it if not exist.
        @param session: SQLAlchemy session object
        @param model: model to query
        @return: row instance
        """
        instance = session.query(model).filter_by(**kwargs).first()
        if instance:
            return instance
        else:
            instance = model(**kwargs)
            return instance

    def set_status(self, task_id, status):
        """Set task status.
        @param task_id: task identifier
        @param status: status string
        @return: operation status
        """
        session = self.Session()
        try:
            row = session.query(Task).get(task_id)
            row.status = status

            if status == TASK_RUNNING:
                row.started_on = datetime.now()
            elif status == TASK_COMPLETED:
                row.completed_on = datetime.now()

            session.commit()
        except SQLAlchemyError as e:
            log.debug("Database error setting status: {0}".format(e))
            session.rollback()
        finally:
            session.close()

    def fetch(self, lock=True):
        """Fetches a task waiting to be processed and locks it for running.
        @return: None or task
        """
        session = self.Session()
        row = None

        try:
            row = session.query(Task).filter(Task.status == TASK_PENDING).order_by("priority desc, added_on").first()

            if not row:
                return None

            if lock:
                self.set_status(task_id=row.id, status=TASK_RUNNING)
                session.refresh(row)
        except SQLAlchemyError as e:
            log.debug("Database error fetching task: {0}".format(e))
            session.rollback()
        finally:
            session.close()

        return row


    def add_error(self, message, task_id):
        """Add an error related to a task.
        @param message: error message
        @param task_id: ID of the related task
        """
        session = self.Session()
        error = Error(message=message, task_id=task_id)
        session.add(error)
        try:
            session.commit()
        except SQLAlchemyError as e:
            log.debug("Database error adding error log: {0}".format(e))
            session.rollback()
        finally:
            session.close()

    # The following functions are mostly used by external utils.

    def add(self,
            obj,
            timeout=0,
            package="",
            options="",
            priority=1,
            custom="",
            platform="",
            tags=None,
            memory=False,
            enforce_timeout=False,
            clock=None):
        """Add a task to database.
        @param obj: object to add (File or URL).
        @param timeout: selected timeout.
        @param options: analysis options.
        @param priority: analysis priority.
        @param custom: custom options.
        @param platform: platform.
        @param tags: optional tags that must be set for machine selection
        @param memory: toggle full memory dump.
        @param enforce_timeout: toggle full timeout execution.
        @param clock: virtual machine clock time
        @return: cursor or None.
        """
        session = self.Session()

        # Convert empty strings and None values to a valid int
        if not timeout:
            timeout = 0
        if not priority:
            priority = 1

        if isinstance(obj, File):
            sample = Sample(md5=obj.get_md5(),
                            crc32=obj.get_crc32(),
                            sha1=obj.get_sha1(),
                            sha256=obj.get_sha256(),
                            sha512=obj.get_sha512(),
                            file_size=obj.get_size(),
                            file_type=obj.get_type(),
                            ssdeep=obj.get_ssdeep())
            session.add(sample)

            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                try:
                    sample = session.query(Sample).filter(Sample.md5 == obj.get_md5()).first()
                except SQLAlchemyError:
                    session.close()
                    return None
            except SQLAlchemyError as e:
                log.debug("Database error adding task: {0}".format(e))
                session.close()
                return None

            task = Task(obj.file_path)
            task.sample_id = sample.id
        elif isinstance(obj, URL):
            task = Task(obj.url)

        task.category = obj.__class__.__name__.lower()
        task.timeout = timeout
        task.package = package
        task.options = options
        task.priority = priority
        task.custom = custom
        task.platform = platform
        task.memory = memory
        task.enforce_timeout = enforce_timeout

        # Deal with tags format (i.e. foo,bar,baz)
        if tags:
            for tag in tags.replace(" ","").split(","):
                task.tags.append(self._get_or_create(session, Tag, name=tag))

        if clock:
            if isinstance(clock, str) or isinstance(clock, unicode):
                try:
                    task.clock = datetime.strptime(clock, "%m-%d-%Y %H:%M:%S")
                except ValueError:
                    log.warning("The date you specified has an invalid format, using current timestamp")
                    task.clock = datetime.now()
            else:
                task.clock = clock

        session.add(task)

        try:
            session.commit()
            task_id = task.id
        except SQLAlchemyError as e:
            log.debug("Database error adding task: {0}".format(e))
            session.rollback()
            return None
        finally:
            session.close()

        return task_id

    def add_path(self,
                 file_path,
                 timeout=0,
                 package="",
                 options="",
                 priority=1,
                 custom="",
                 platform="",
                 tags=None,
                 memory=False,
                 enforce_timeout=False,
                 clock=None):
        """Add a task to database from file path.
        @param file_path: sample path.
        @param timeout: selected timeout.
        @param options: analysis options.
        @param priority: analysis priority.
        @param custom: custom options.
        @param platform: platform.
        @param tags: Tags required in machine selection
        @param memory: toggle full memory dump.
        @param enforce_timeout: toggle full timeout execution.
        @param clock: virtual machine clock time
        @return: cursor or None.
        """
        if not file_path or not os.path.exists(file_path):
            return None
        
        # Convert empty strings and None values to a valid int
        if not timeout:
            timeout = 0
        if not priority:
            priority = 1

        return self.add(File(file_path),
                        timeout,
                        package,
                        options,
                        priority,
                        custom,
                        platform,
                        tags,
                        memory,
                        enforce_timeout,
                        clock)

    def add_url(self,
                url,
                timeout=0,
                package="",
                options="",
                priority=1,
                custom="",
                platform="",
                tags=None,
                memory=False,
                enforce_timeout=False,
                clock=None):
        """Add a task to database from url.
        @param url: url.
        @param timeout: selected timeout.
        @param options: analysis options.
        @param priority: analysis priority.
        @param custom: custom options.
        @param platform: platform.
        @param tags: tags for machine selection
        @param memory: toggle full memory dump.
        @param enforce_timeout: toggle full timeout execution.
        @param clock: virtual machine clock time
        @return: cursor or None.
        """
        
        # Convert empty strings and None values to a valid int
        if not timeout:
            timeout = 0
        if not priority:
            priority = 1
        
        return self.add(URL(url),
                        timeout,
                        package,
                        options,
                        priority,
                        custom,
                        platform,
                        tags,
                        memory,
                        enforce_timeout,
                        clock)

    def reschedule(self, task_id):
        """Reschedule a task.
        @param task_id: ID of the task to reschedule.
        @return: ID of the newly created task.
        """
        task = self.view_task(task_id)

        if not task:
            return None

        if task.category == "file":
            add = self.add_path
        elif task.category == "url":
            add = self.add_url

        # Change status to recovered.
        session = self.Session()
        session.query(Task).get(task_id).status = TASK_RECOVERED
        try:
            session.commit()
        except SQLAlchemyError as e:
            log.debug("Database error rescheduling task: {0}".format(e))
            session.rollback()
            return False
        finally:
            session.close()

        # Normalize tags.
        if task.tags:
            tags = ",".join([tag.name for tag in task.tags])
        else:
            tags = task.tags

        return add(task.target,
                   task.timeout,
                   task.package,
                   task.options,
                   task.priority,
                   task.custom,
                   task.platform,
                   tags,
                   task.memory,
                   task.enforce_timeout,
                   task.clock)

    def list_tasks(self, limit=None, details=False, category=None, offset=None, status=None, not_status=None):
        """Retrieve list of task.
        @param limit: specify a limit of entries.
        @param details: if details about must be included
        @param category: filter by category
        @param offset: list offset
        @param status: filter by task status
        @param not_status: exclude this task status from filter
        @return: list of tasks.
        """
        session = self.Session()
        try:
            search = session.query(Task)

            if status:
                search = search.filter(Task.status == status)
            if not_status:
                search = search.filter(Task.status != not_status)
            if category:
                search = search.filter(Task.category == category)
            if details:
                search = search.options(joinedload("errors"), joinedload("tags"))

            tasks = search.order_by("added_on desc").limit(limit).offset(offset).all()
        except SQLAlchemyError as e:
            log.debug("Database error listing tasks: {0}".format(e))
            print e
            return None
        finally:
            session.close()
        return tasks

    def count_tasks(self, status=None):
        """Count tasks in the database
        @param status: apply a filter according to the task status
        @return: number of tasks found
        """
        session = self.Session()
        try:
            if status:
                tasks_count = session.query(Task).filter(Task.status == status).count()
            else:
                tasks_count = session.query(Task).count()
        except SQLAlchemyError as e:
            log.debug("Database error counting tasks: {0}".format(e))
            return 0
        finally:
            session.close()
        return tasks_count

    def view_task(self, task_id, details=False):
        """Retrieve information on a task.
        @param task_id: ID of the task to query.
        @return: details on the task.
        """
        session = self.Session()
        try:
            if details:
                task = session.query(Task).options(joinedload("errors"), joinedload("tags")).get(task_id)
            else:
                task = session.query(Task).get(task_id)
        except SQLAlchemyError as e:
            log.debug("Database error viewing task: {0}".format(e))
            return None
        else:
            if task:
                session.expunge(task)
        finally:
            session.close()
        return task

    def delete_task(self, task_id):
        """Delete information on a task.
        @param task_id: ID of the task to query.
        @return: operation status.
        """
        session = self.Session()
        try:
            task = session.query(Task).get(task_id)
            session.delete(task)
            session.commit()
        except SQLAlchemyError as e:
            log.debug("Database error deleting task: {0}".format(e))
            session.rollback()
            return False
        finally:
            session.close()
        return True

    def view_sample(self, sample_id):
        """Retrieve information on a sample given a sample id.
        @param sample_id: ID of the sample to query.
        @return: details on the sample used in sample: sample_id.
        """
        session = self.Session()
        try:
            sample = session.query(Sample).get(sample_id)
        except AttributeError:
            return None
        except SQLAlchemyError as e:
            log.debug("Database error viewing task: {0}".format(e))
            return None
        else:
            if sample:
                session.expunge(sample)
        finally:
            session.close()

        return sample

    def find_sample(self, md5=None, sha256=None):
        """Search samples by MD5.
        @param md5: md5 string
        @return: matches list
        """
        session = self.Session()
        try:
            if md5:
                sample = session.query(Sample).filter(Sample.md5 == md5).first()
            elif sha256:
                sample = session.query(Sample).filter(Sample.sha256 == sha256).first()
        except SQLAlchemyError as e:
            log.debug("Database error searching sample: {0}".format(e))
            return None
        else:
            if sample:
                session.expunge(sample)
        finally:
            session.close()
        return sample

    def count_samples(self):
        """Counts the amount of samples in the database."""
        session = self.Session()
        try:
            sample_count = session.query(Sample).count()
        except SQLAlchemyError as e:
            log.debug("Database error counting samples: {0}".format(e))
            return 0
        finally:
            session.close()
        return sample_count

    def view_errors(self, task_id):
        """Get all errors related to a task.
        @param task_id: ID of task associated to the errors
        @return: list of errors.
        """
        session = self.Session()
        try:
            errors = session.query(Error).filter(Error.task_id == task_id).all()
        except SQLAlchemyError as e:
            log.debug("Database error viewing errors: {0}".format(e))
            return None
        finally:
            session.close()
        return errors
