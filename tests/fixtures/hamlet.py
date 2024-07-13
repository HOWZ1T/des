import os.path
from typing import List

import nltk
import pytest


@pytest.fixture(scope="session")
def hamlet(request) -> List[str]:
    """
    Load the content of the hamlet file and tokenize the content into an array of words.

    Args:
        request: the pytest request object.

    Returns:
        List: the array of words in the hamlet file.
    """
    fixture_data_dir_fp = request.config.getoption("--fixture-data-dir")
    assert os.path.exists(
        fixture_data_dir_fp
    ), f"fixture data directory not found: {fixture_data_dir_fp}"

    fp = os.path.join(fixture_data_dir_fp, "hamlet.txt")
    assert os.path.exists(fp), f"hamlet file not found: {fp}"

    tokenizer = nltk.tokenize.TreebankWordTokenizer()
    with open(fp, "r") as f:
        content = f.read()
        words = tokenizer.tokenize(content)

    return words
