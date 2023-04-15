from __future__ import annotations

import os
import tempfile
from abc import ABC, abstractmethod
from ast import literal_eval
from bisect import bisect_left
from collections import defaultdict
from multiprocessing import Pool
from pathlib import Path
from typing import Union, Iterable, Callable, Optional, List

from psutil import Process, virtual_memory
from tqdm import tqdm


class ChunkWriter:
    def __init__(self, prefix, chunk_size_lines=1000000):
        prefix = Path(prefix)

        assert prefix.is_dir()

        self._prefix = prefix
        self._parts = []
        self._opened_file = None
        self._chunk_size_lines = chunk_size_lines

    def get_chunks(self):
        return self._parts

    def _close_last_chunk(self):
        if self._opened_file is not None and self._opened_file.closed is False:
            self._opened_file.close()
            self._opened_file = None

    def _open_next_chunk(self):
        ind = len(self._parts)
        ind_num = f"{ind:5d}".replace(" ", "0")
        file_path = self._prefix.joinpath(f"part_{ind_num}")
        self._parts.append(file_path)

        self._close_last_chunk()
        self._opened_file = open(file_path, "a")
        self._written = 0

    def dump_single(self, item):
        if self._opened_file is None:
            self._open_next_chunk()

        self._opened_file.write(f"{item}\n")
        self._written += 1

        if self._written >= self._chunk_size_lines:
            self._open_next_chunk()

        return item

    def finalize(self):
        self._close_last_chunk()


class MapReduceNode(ABC):
    """
    Base class for map and reduce operations
    """
    _data_source: Union[Iterable, MapReduceNode]
    _map_fn: Optional[Callable]
    _reduce_fn: Optional[Callable]
    _persist: bool
    _path: Optional[Path]
    _parent: Optional[MapReduceNode]
    _children: List[MapReduceNode]

    def __init__(
            self, data_source: Union[Iterable, MapReduceNode], map_fn: Optional[Callable] = None,
            reduce_fn: Optional[Callable] = None, path: Optional[Path] = None, stage_name=None
    ):
        """
        Create an instance of MapReduceNode
        :param data_source: can be either a MapReduceNode from the previous stage or an iterable
        :param map_fn: Map function. Used only from Map, FlatMap and Filter jobs. Should be serializable when using
        parallel map
        :param reduce_fn: Reduce function. Used only for Reduce jobs
        :param path: Path where intermediate results are stored. Used for Cache job
        """
        if not isinstance(data_source, MapReduceNode):
            assert map_fn is None and reduce_fn is None, f"map and reduce are invalid for the data source"
        else:
            if type(self) not in {CacheJob, PersistJob, SortJob}:
                assert (map_fn is not None) != (reduce_fn is not None), \
                    f"Should specify either map or reduce functions, given: map {map_fn}, reduce {reduce_fn}"

        self._data_source = data_source
        self._map_fn = map_fn
        self._reduce_fn = reduce_fn
        self._persist = False
        self._path = None if path is None else Path(path)
        self.stage_name = stage_name

        if self._path is not None:
            self._persist = True
            if self._path.is_dir():
                if type(self) != CacheJob:
                    raise Exception(f"Output directory exists: {self._path}")
                self._parent = TextSource(self._path)
            self._path.mkdir(exist_ok=True)
            self._writer = ChunkWriter(self._path)

        self._stage_id = 0
        if isinstance(data_source, MapReduceNode):
            self._parent = data_source
            self._parent.register_child(self)
            self._stage_id = self._parent._get_stage_id() + 1
        else:
            self._parent = None

        self._children = []

        self._stream = None

    def _get_stage_id(self):
        return self._stage_id

    @staticmethod
    def _get_stage_name(stage: MapReduceNode):
        if stage.stage_name is not None:
            return f"{stage.stage_name} at Stage {stage._get_stage_id()}"
        return f"Stage {stage._get_stage_id()}"

    def _get_temp_path(self):
        return Path(tempfile.gettempdir()).joinpath(self._get_random_name())

    @staticmethod
    def _get_random_name(length=10):
        char_ranges = [chr(i) for i in range(ord("a"), ord("a") + 26)] + \
                      [chr(i) for i in range(ord("A"), ord("A") + 26)] + \
                      [chr(i) for i in range(ord("0"), ord("0") + 10)]
        from random import sample
        return "".join(sample(char_ranges, k=length))

    def register_child(self, node):
        if len(self._children) == 1:
            self._persist = True
            raise NotImplementedError("Only sequential MapReduce is implemented so far")
        self._children.append(node)

    @abstractmethod
    def _init_job(self):
        yield from ...

    def __iter__(self):
        self._stream = self._init_job()
        return self

    def __next__(self):
        return self._get_next()

    def _get_next(self):
        return next(self._stream)

    def map(self, map_fn, allow_parallel=False, n_workers=4, **kwargs):
        return MapJob(self, map_fn=map_fn, allow_parallel=allow_parallel, n_workers=n_workers, **kwargs)

    def flat_map(self, map_fn, allow_parallel=False, n_workers=4, **kwargs):
        return FlatMapJob(self, map_fn=map_fn, allow_parallel=allow_parallel, n_workers=n_workers, **kwargs)

    def filter(self, filter_fn):
        return FilterJob(self, map_fn=filter_fn)

    def reduce(self, reduce_fn, **kwargs):
        return ReduceJob(self, reduce_fn=reduce_fn, **kwargs)

    def persist(self, path, serialize_fn=None):
        return PersistJob(self, path=path, serialize_fn=serialize_fn)

    def cache(self, path):
        return CacheJob(self, path=path)

    def sort(
            self, key_fn=None, ascending=True, chunk_size_lines=100000, serialize_fn=None, deserialize_fn=None
    ):
        return SortJob(
            self, key_fn=key_fn, ascending=ascending, chunk_size_lines=chunk_size_lines,
            serialize_fn=serialize_fn, deserialize_fn=deserialize_fn
        )


class DataSource(MapReduceNode):
    def __init__(self, data_source: Union[Iterable, MapReduceNode]):
        """
        Node that should be used at the beginning of any sequence of MapReduce jobs
        :param data_source: An iterable that streams the data
        """
        super().__init__(data_source)

    def _init_job(self):
        yield from self._data_source


class TextSource(MapReduceNode):
    _deserialize_fn: Optional[Callable]

    def __init__(self, path, deserialize_fn=None):
        """
        Create data source from a file
        :param path: path to the file, each line should contain a single entry
        :param deserialize_fn: Callable to deserialize an entry. If not provided, data is treated as plain text
        """
        self._deserialize_fn = deserialize_fn
        super().__init__(self._read_text(path))

    def _read_text(self, path):
        path = Path(path)
        for file in path.iterdir():
            if file.is_dir() or file.name.startswith("."):
                continue
            for line in open(file):
                if line == "":
                    continue
                item = line[:-1]
                if self._deserialize_fn is not None:
                    item = self._deserialize_fn(item)
                yield item

    def _init_job(self):
        yield from self._data_source


class MapJob(MapReduceNode):
    _allow_parallel: bool
    _pool = None
    _chunk_size: int

    def __init__(self, *args, allow_parallel=False, chunk_size=4000, n_workers=4, **kwargs):
        """
        Create a Map job
        :param args: arguments for MapReduceNode
        :param allow_parallel: if True, create a multiprocessing pool. Keep in mind that the task should be difficult
        enough, otherwise compute is wasted on overhead from job distribution
        :param chunk_size: when `allow_parallel` is True, process data in chunks of specified size
        :param n_workers: number of workers for parallel processing
        :param kwargs: arguments for MapReduceNode
        """
        super().__init__(*args, **kwargs)
        self._allow_parallel = allow_parallel
        self._pool = Pool(n_workers) if allow_parallel else None
        self._chunk_size = chunk_size

    def _get_next(self):
        try:
            return next(self._stream)
        except StopIteration:
            if self._allow_parallel:
                self._pool.close()
            raise StopIteration

    def _process_in_parallel(self, stream):
        buffer = []

        for item in stream:
            buffer.append(item)

            if len(buffer) >= self._chunk_size:
                yield from self._pool.imap_unordered(self._map_fn, buffer)
                buffer.clear()

        if len(buffer) > 0:
            yield from self._pool.imap_unordered(self._map_fn, buffer)
            buffer.clear()

    def _init_job(self):
        assert self._map_fn is not None

        stream = self._parent

        if self._allow_parallel:
            mapped = self._process_in_parallel(stream)
        else:
            mapped = map(self._map_fn, stream)

        yield from mapped


class FilterJob(MapJob):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _init_job(self):
        assert self._map_fn is not None
        filtered = filter(self._map_fn, self._parent)
        yield from filtered


class FlatMapJob(MapJob):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _init_job(self):
        def yield_from_mapped(mapped):
            for m in mapped:
                yield from m

        yield from yield_from_mapped(super()._init_job())


class PersistJob(MapReduceNode):
    _serialize_fn: Optional[Callable]

    def __init__(self, *args, serialize_fn=None, **kwargs):
        """
        Node for writing the output to a file
        :param args: arguments for MapReduceNode
        :param serialize_fn: function for serializing the output. If not specified, the default string representation
        is used
        :param kwargs: arguments for MapReduceNode
        """
        super().__init__(*args, **kwargs)
        self._serialize_fn = serialize_fn
        self._init_job()

    def __iter__(self):
        raise NotImplementedError("PersistJob does not support iteration")

    def _init_job(self):
        for item in self._parent:
            if self._serialize_fn is not None:
                item = self._serialize_fn(item)
            self._writer.dump_single(item)


class CacheJob(MapReduceNode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _init_job(self):
        def cache_and_return(stream):
            for item in stream:
                self._writer.dump_single(item)
                yield item

        yield from cache_and_return(self._parent)


class SortJob(MapReduceNode):
    delimiter = "\t"

    def __init__(
            self, *args, chunk_size_lines=100000, serialize_fn=None, deserialize_fn=None,
            key_fn=None, ascending=True, **kwargs
    ):
        super(SortJob, self).__init__(*args, **kwargs)
        if self._path is None:
            self._path = self._get_temp_path()
            self._path.mkdir()
        self._writer = ChunkWriter(self._path, chunk_size_lines)
        self._ascending = ascending
        self._chunk_size = chunk_size_lines
        self._serialize_fn = serialize_fn
        self._deserialize_fn = deserialize_fn
        self._key_fn = key_fn

        if self._key_fn is None:
            self._key_fn = lambda x: x
        if self._serialize_fn is None:
            self._serialize_fn = lambda obj: f"{obj}"
        if self._deserialize_fn is None:
            self._deserialize_fn = lambda text: literal_eval(text)

    def _init_job(self):
        buffer = []
        for item in self._parent:
            buffer.append(item)
            if len(buffer) == self._chunk_size:
                self._write_sorted_buffer(buffer)
        if len(buffer) > 0:
            self._write_sorted_buffer(buffer)

        yield from self._get_sorted()

    def _serialize(self, value):
        try:
            return self._serialize_fn(value)
        except Exception as e:
            raise Exception(f"Encountered exception during serialization of {value}: {e}")

    def _deserialize(self, value):
        try:
            return self._deserialize_fn(value)
        except Exception as e:
            raise Exception(f"Encountered exception during deserialization of {value}: {e}")

    def _write_sorted_buffer(self, buffer):
        buffer.sort(key=self._key_fn, reverse=not self._ascending)
        for item in buffer:
            self._writer.dump_single(self._serialize(item))
        # self._writer.finalize()
        buffer.clear()

    def _merge_files(self, files, level, ind):
        sorted_path = self._path.joinpath(f"sorted_{level}_{ind}")

        opened_files = []
        item_frontier = []
        for file_ in files:
            opened_file = open(file_, "r")
            item = opened_file.readline()
            if item == "":
                continue
            opened_files.append(opened_file)
            item_frontier.append(item)

        items = list(zip(
            list(range(len(item_frontier))),
            [self._key_fn(self._deserialize(item)) for item in item_frontier],
            item_frontier
        ))
        items.sort(key=lambda x: x[1])  # sort by key
        keys = [item[1] for item in items]  # need this because bisect_left does not have key argument in python 3.8

        del item_frontier

        pop_item = 0 if self._ascending else -1
        with open(sorted_path, "w") as sink:
            while len(items) > 0:
                next_ind, _, next_item = items.pop(pop_item)
                _ = keys.pop(pop_item)
                sink.write(next_item)

                new_item = opened_files[next_ind].readline()

                if new_item == "":
                    opened_files[next_ind].close()
                else:
                    new_key = self._key_fn(self._deserialize(new_item))
                    insert_position = bisect_left(keys, new_key)
                    items.insert(insert_position, (next_ind, new_key, new_item))
                    keys.insert(insert_position, new_key)

        return sorted_path

    def _merge_sorted(self, files, merge_chunks=20, level=0):
        if len(files) == 1:
            return files[0]

        to_merge = []
        merged = []

        while len(files) > 0:
            to_merge.append(files.pop(0))

            if len(to_merge) >= merge_chunks or len(files) == 0:
                merged.append(self._merge_files(to_merge, level, ind=len(merged)))
                for file in to_merge:
                    os.remove(file)
                to_merge.clear()

        return self._merge_sorted(merged, level=level + 1)

    def _sort(self):
        parts = self._writer.get_chunks()
        assert len(parts) > 0

        return self._merge_sorted(parts)

    def _get_sorted(self):
        self._writer.finalize()

        sorted_path = self._sort()
        with open(sorted_path, "r") as sorted_:
            for line in sorted_:
                yield self._deserialize_fn(line.strip())
                
                
class _PreReduce(MapReduceNode):
    _memory_check_frequency: int
    _free_memory_thresh_mb: Union[float, int]
    _taken_memory_thresh_mb: Union[float, int]
    
    def __init__(
            self, *args, memory_check_frequency=1000000, free_memory_thresh_mb=500,
            taken_memory_thresh_mb=4000, **kwargs
    ):
        super(_PreReduce, self).__init__(*args, **kwargs)
        self._memory_check_frequency = memory_check_frequency
        self._free_memory_thresh_mb = free_memory_thresh_mb
        self._taken_memory_thresh_mb = taken_memory_thresh_mb

    @staticmethod
    def _create_buffer_storage():
        return defaultdict(lambda: None)

    @staticmethod
    def _get_occupied_memory_mb():
        return Process(os.getpid()).memory_info().rss / 1024 / 1024

    @staticmethod
    def _get_free_memory_mb():
        return virtual_memory().available / 1024 / 1024

    def _reduce(self, accumulator, value):
        if accumulator is None:
            return value
        return self._reduce_fn(accumulator, value)

    def _init_job(self):
        temp_storage_shard = self._create_buffer_storage()

        processed = 0
        for ind, value in enumerate(tqdm(self._parent, desc=self._get_stage_name(self._parent))):
            if ind == 0:
                if not (isinstance(value, tuple) and len(value) == 2):
                    raise TypeError(f"Reduce Job expects a tuple of length 2 with key and value, but received: {value}")

            map_id, map_val = value

            temp_storage_shard[map_id] = self._reduce(
                temp_storage_shard[map_id], map_val
            )
            processed += 1

            if processed % self._memory_check_frequency == 0:
                if (
                        self._get_occupied_memory_mb() >= self._taken_memory_thresh_mb or
                        self._get_free_memory_mb() < self._free_memory_thresh_mb
                ):
                    yield from temp_storage_shard.items()
                    temp_storage_shard.clear()

        if len(temp_storage_shard) > 0:
            yield from temp_storage_shard.items()
            temp_storage_shard.clear()


class ReduceJob(MapReduceNode):
    _memory_check_frequency: int
    _free_memory_thresh_mb: Union[float, int]
    _taken_memory_thresh_mb: Union[float, int]

    def __init__(
            self, *args, memory_check_frequency=1000000, free_memory_thresh_mb=500,
            taken_memory_thresh_mb=4000, serialize_fn=None, deserialize_fn=None,
            sort_job_chink_size=1000000,
            **kwargs
    ):
        """
        Create a reduce job. Keeps trying to keep the result in-memory unless a limit on free memory or a limit
        on consumed memory is exceeded
        :param args: arguments for MapReduceNode
        :param memory_check_frequency: The frequency for checking consumed and free memory. One iteration is one call
        of `reduce_fn`
        :param free_memory_thresh_mb: Dump to disk when free memory is less than specified
        :param taken_memory_thresh_mb: Dump to disk when the memory taken by the process is more than specified
        :param shuffler_class: The shuffler class to use before the final reduce
        :param kwargs: arguments for MapReduceNode
        """
        self._pre_reduce_job = _PreReduce(
            args[0], reduce_fn=kwargs["reduce_fn"], memory_check_frequency=memory_check_frequency,
            free_memory_thresh_mb=free_memory_thresh_mb, taken_memory_thresh_mb=taken_memory_thresh_mb,
        )
        sort_job = SortJob(
            self._pre_reduce_job, chunk_size_lines=sort_job_chink_size,
            serialize_fn=serialize_fn, deserialize_fn=deserialize_fn,
            key_fn=lambda x: x[0], ascending=True
        )
        super().__init__(sort_job, **kwargs)

    def _reduce(self, accumulator, value):
        if accumulator is None:
            return value
        return self._reduce_fn(accumulator, value)

    def _init_job(self):
        last_key = None
        reduced_value = None
        for ind, (key, value) in enumerate(tqdm(self._parent, desc=self._get_stage_name(self))):
            if last_key != key and last_key is not None:
                yield last_key, reduced_value
                reduced_value = None

            reduced_value = self._reduce(reduced_value, value)
            last_key = key

        if last_key is not None:
            yield last_key, reduced_value
