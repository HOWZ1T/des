"""
REFERENCE PAPER:
  "Distinct Elements in Streams: An Algorithm for the (Text) Book" ( https://arxiv.org/pdf/2301.10191 )
"""

import math
import random
import threading
from typing import Callable, Iterable, Iterator, Optional

_rel_err_tol_default: float = 0.1
_fail_prob_default: float = 0.05

# threshold must be an int or a function that takes in the size (number of items) in the stream, and optional floats
# which represent the relative error tolerance and failure probability in the default threshold function.
_threshold_type = int | Callable[[int, Optional[float], Optional[float]], int]


def calculate_threshold(
    stream_size: int, relative_error_tolerance: float, failure_probability: float
) -> int:
    """
    Calculate the threshold for the DES algorithm based on the stream size, relative error tolerance,
    and failure probability.

    Args:
        stream_size (int): the number of items in the stream.
        relative_error_tolerance (float): the relative error tolerance for the algorithm.
        failure_probability (float): the failure probability for the algorithm.

    Returns:
        int: the threshold value for the DES algorithm.

    Raises:
        ValueError: if the relative error tolerance or failure probability are not in the range (0, 1),
            or if the stream size is less than or equal to 0.
    """
    if relative_error_tolerance <= 0 or relative_error_tolerance >= 1:
        raise ValueError(
            f"Relative error tolerance must be in the range (0, 1), got: {relative_error_tolerance}"
        )

    if failure_probability <= 0 or failure_probability >= 1:
        raise ValueError(
            f"Failure probability must be in the range (0, 1), got: {failure_probability}"
        )

    if stream_size <= 0:
        raise ValueError(f"Stream size must be greater than 0, got: {stream_size}")

    return math.ceil(
        (12.0 / math.pow(relative_error_tolerance, 2))
        * math.log((8.0 * stream_size) / failure_probability)
    )


class DES:
    _threshold: _threshold_type
    _threshold_hit_count: int
    relative_error_tolerance: float
    failure_probability: float
    seed: int
    acc_set: set  # this set accumulates the distinct elements and maintains that it's size is less than the threshold
    cur_probability: float
    rand: random.Random
    distinct_item_count: int
    rlock: threading.RLock

    def __init__(
        self,
        threshold: _threshold_type = calculate_threshold,
        relative_error_tolerance: float = _rel_err_tol_default,
        failure_probability: float = _fail_prob_default,
        seed: int | None = None,
    ):
        self._threshold = threshold
        self.relative_error_tolerance = relative_error_tolerance
        self.failure_probability = failure_probability
        self.seed = seed if seed is not None else random.randint(0, (2**32 - 1))
        self.rlock = threading.RLock()

        self.reset()

    def reset(self):
        with self.rlock:
            self.distinct_item_count = 0
            self._threshold_hit_count = 0
            self.rand = random.Random(self.seed)

            # initialize probability and distinct word set
            self.acc_set = set()
            self.cur_probability = 1.0

    def get_threshold(self, stream_size: int) -> int:
        return (
            (
                self._threshold(
                    stream_size, self.relative_error_tolerance, self.failure_probability
                )
            )
            if callable(self._threshold)
            else self._threshold
        )

    def get_threshold_hit_count(self) -> int:
        with self.rlock:
            return self._threshold_hit_count

    def distinct(
        self,
        stream: Iterator | Iterable,
        stream_size: int,
        reset_state: bool = False,
        poll_rate: int = 10,  # updates the distinct element count every poll_rate items (e.g. every 10 items)
    ) -> int:
        """
        Calculate the distinct elements in a stream using the DES algorithm.

        Args:
            stream (Iterator | Iterable): the stream of items to process.
            stream_size (int): the number of items in the stream.
            reset_state (bool): whether to reset the state of the algorithm before processing the stream.
            poll_rate (int): the rate at which to update the distinct element count (based on the number of items processed).

        Returns:
            int: the estimated number of distinct elements in the stream.
        """
        if reset_state is True:
            self.reset()

        # calculate threshold
        thresh = (
            self._threshold(
                stream_size, self.relative_error_tolerance, self.failure_probability
            )
            if callable(self._threshold)
            else self._threshold
        )

        assert thresh > 0, "Threshold must be greater than 0"

        if isinstance(stream, Iterable):
            stream = iter(stream)

        i = 0
        while (item := next(stream, None)) is not None:
            with self.rlock:
                if item in self.acc_set:
                    self.acc_set.remove(item)

                if self.rand.random() < self.cur_probability:
                    self.acc_set.add(item)

                if len(self.acc_set) >= thresh:
                    self._threshold_hit_count += 1

                    # down sample set by removing elements with p 1/2
                    self.acc_set = {
                        v for v in self.acc_set if self.rand.random() >= 0.5
                    }
                    self.cur_probability = self.cur_probability / 2.0

                    set_size = len(self.acc_set)
                    if set_size >= thresh:
                        raise OverflowError(
                            f"Could not down sample the set to the desired threshold: {thresh}, set_size: {set_size}"
                        )

            if i % poll_rate == 0:
                with self.rlock:
                    self.distinct_item_count = int(
                        math.floor(len(self.acc_set) / self.cur_probability)
                    )
                    i = 0

            i += 1

        with self.rlock:
            self.distinct_item_count = int(
                math.floor(len(self.acc_set) / self.cur_probability)
            )

        return self.distinct_item_count
