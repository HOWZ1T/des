import timeit
from typing import List

import pytest

from des.algorithm import DES


@pytest.mark.unit
def test_distinct_element_count_algorithm(hamlet: List[str]):
    def run_test(accuracies_accumulator: List[float]):
        des = DES(threshold=100, seed=42)
        count = des.distinct(hamlet, len(hamlet))
        actual_distinct_count = len(set(hamlet))

        act_thresh = des.get_threshold(len(hamlet))
        exp_thresh = 100
        assert (
            act_thresh == exp_thresh
        ), f"expected threshold of {exp_thresh}, got: {des.get_threshold(len(hamlet))}"

        act_thresh_hit_count = des.get_threshold_hit_count()
        exp_thresh_hit_count = 6
        assert (
            act_thresh_hit_count == exp_thresh_hit_count
        ), f"expected threshold hit count of {exp_thresh_hit_count}, got: {des.get_threshold_hit_count()}"

        # calculate accuracy
        error = abs(actual_distinct_count - count)
        accuracy = 1.0 - (error / actual_distinct_count)
        accuracies_accumulator.append(accuracy)

    accuracies = []
    run_times = timeit.repeat(lambda: run_test(accuracies), repeat=10, number=100)

    avg_accuracy = sum(accuracies) / len(accuracies)
    avg_run_time = sum(run_times) / len(run_times)

    print(f"average accuracy: {avg_accuracy}")
    print(f"average run time: {avg_run_time} seconds")

    assert (
        avg_accuracy >= 0.9
    ), f"expected accuracy of at least 0.9, got: {avg_accuracy}"
