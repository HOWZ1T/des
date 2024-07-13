from tests.fixtures.hamlet import *  # noqa: F403, F401 (ignore as this is needed for pytest to register the fixture)

# -- PYTEST CLI ADDOPTIONS --


def pytest_addoption(parser):
    """
    Add command line options to the pytest CLI.
    """
    parser.addoption(
        "--fixture-data-dir",
        action="store",
        default="./tests/fixtures/data",
        help="the path to the directory containing fixture data",
    )
