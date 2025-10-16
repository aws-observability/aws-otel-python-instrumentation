# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sysconfig
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

from amazon.opentelemetry.distro.code_correlation.internal.packages_resolver import (
    Distribution,
    _build_package_mapping,
    _determine_effective_root,
    _extract_root_module_name,
    _load_third_party_packages,
    _resolve_system_paths,
    _validate_void_function,
    execute_once,
    is_standard_library,
    is_third_party_package,
    is_user_code,
    resolve_package_from_filename,
)


class TestDistribution(TestCase):
    """Test the Distribution NamedTuple."""

    def test_distribution_initialization(self):
        """Test Distribution initialization with name and version."""
        dist = Distribution(name="test-package", version="1.0.0")

        self.assertEqual(dist.name, "test-package")
        self.assertEqual(dist.version, "1.0.0")

    def test_distribution_equality(self):
        """Test Distribution equality comparison."""
        dist1 = Distribution(name="package", version="1.0")
        dist2 = Distribution(name="package", version="1.0")
        dist3 = Distribution(name="package", version="2.0")

        self.assertEqual(dist1, dist2)
        self.assertNotEqual(dist1, dist3)

    def test_distribution_tuple_behavior(self):
        """Test Distribution behaves as a tuple."""
        dist = Distribution(name="pkg", version="1.0")

        # Can be unpacked like a tuple
        name, version = dist
        self.assertEqual(name, "pkg")
        self.assertEqual(version, "1.0")

        # Can be indexed like a tuple
        self.assertEqual(dist[0], "pkg")
        self.assertEqual(dist[1], "1.0")

    def test_distribution_repr(self):
        """Test Distribution string representation."""
        dist = Distribution(name="test-pkg", version="2.5.1")
        repr_str = repr(dist)

        self.assertIn("test-pkg", repr_str)
        self.assertIn("2.5.1", repr_str)


class TestValidateVoidFunction(TestCase):
    """Test the _validate_void_function helper."""

    def test_validate_void_function_valid(self):
        """Test validation of function with no arguments."""

        def valid_func():
            return "test"

        from inspect import getfullargspec

        argspec = getfullargspec(valid_func)
        result = _validate_void_function(valid_func, argspec)

        self.assertTrue(result)

    def test_validate_void_function_with_args(self):
        """Test validation fails for function with positional arguments."""

        def func_with_args(arg):
            return arg

        from inspect import getfullargspec

        argspec = getfullargspec(func_with_args)
        result = _validate_void_function(func_with_args, argspec)

        self.assertFalse(result)

    def test_validate_void_function_with_kwargs(self):
        """Test validation fails for function with keyword arguments."""

        def func_with_kwargs(**kwargs):
            return kwargs

        from inspect import getfullargspec

        argspec = getfullargspec(func_with_kwargs)
        result = _validate_void_function(func_with_kwargs, argspec)

        self.assertFalse(result)

    def test_validate_void_function_with_varargs(self):
        """Test validation fails for function with varargs."""

        def func_with_varargs(*args):
            return args

        from inspect import getfullargspec

        argspec = getfullargspec(func_with_varargs)
        result = _validate_void_function(func_with_varargs, argspec)

        self.assertFalse(result)

    def test_validate_void_function_with_defaults(self):
        """Test validation fails for function with default arguments."""

        def func_with_defaults(arg="default"):
            return arg

        from inspect import getfullargspec

        argspec = getfullargspec(func_with_defaults)
        result = _validate_void_function(func_with_defaults, argspec)

        self.assertFalse(result)

    def test_validate_void_function_generator(self):
        """Test validation fails for generator function."""

        def generator_func():
            yield 1

        from inspect import getfullargspec

        argspec = getfullargspec(generator_func)
        result = _validate_void_function(generator_func, argspec)

        self.assertFalse(result)


class TestExecuteOnce(TestCase):
    """Test the execute_once decorator."""

    def test_execute_once_valid_function(self):
        """Test execute_once decorator on valid function."""
        call_count = 0

        @execute_once
        def test_func():
            nonlocal call_count
            call_count += 1
            return "result"

        # First call
        result1 = test_func()
        self.assertEqual(result1, "result")
        self.assertEqual(call_count, 1)

        # Second call should return cached result
        result2 = test_func()
        self.assertEqual(result2, "result")
        self.assertEqual(call_count, 1)  # Should not increment

    def test_execute_once_with_exception(self):
        """Test execute_once decorator when function raises exception."""
        call_count = 0

        @execute_once
        def failing_func():
            nonlocal call_count
            call_count += 1
            raise ValueError("test error")

        # First call should raise exception
        with self.assertRaises(ValueError):
            failing_func()
        self.assertEqual(call_count, 1)

        # Second call should raise the same exception without re-executing
        with self.assertRaises(ValueError):
            failing_func()
        self.assertEqual(call_count, 1)  # Should not increment

    def test_execute_once_invalid_function(self):
        """Test execute_once decorator rejects function with arguments."""
        with self.assertRaises(ValueError) as context:

            @execute_once
            def invalid_func(arg):
                return arg

        self.assertIn("no arguments", str(context.exception))

    def test_execute_once_function_attributes(self):
        """Test execute_once preserves function attributes."""

        @execute_once
        def test_func():
            """Test docstring."""
            return "result"

        self.assertEqual(test_func.__name__, "test_func")
        self.assertEqual(test_func.__doc__, "Test docstring.")


class TestDetermineEffectiveRoot(TestCase):
    """Test the _determine_effective_root function."""

    def test_determine_effective_root_package_with_init(self):
        """Test determining root for package with __init__.py."""
        with patch("pathlib.Path.is_dir", return_value=True), patch("pathlib.Path.exists", return_value=True):

            relative_path = Path("mypackage/submodule.py")
            parent_path = Path("/usr/lib/python3.9/site-packages")

            result = _determine_effective_root(relative_path, parent_path)
            self.assertEqual(result, "mypackage")

    def test_determine_effective_root_no_init(self):
        """Test determining root for module without __init__.py."""
        with patch("pathlib.Path.is_dir", return_value=False):
            relative_path = Path("module/file.py")
            parent_path = Path("/usr/lib/python3.9/site-packages")

            result = _determine_effective_root(relative_path, parent_path)
            self.assertEqual(result, "module/file.py")

    def test_determine_effective_root_directory_no_init(self):
        """Test determining root for directory without __init__.py."""
        with patch("pathlib.Path.is_dir", return_value=True), patch("pathlib.Path.exists", return_value=False):

            relative_path = Path("namespace/subpackage.py")
            parent_path = Path("/usr/lib/python3.9/site-packages")

            result = _determine_effective_root(relative_path, parent_path)
            self.assertEqual(result, "namespace/subpackage.py")


class TestResolveSystemPaths(TestCase):
    """Test the _resolve_system_paths function."""

    def test_resolve_system_paths_caching(self):
        """Test that system paths are cached properly."""
        with patch(
            "amazon.opentelemetry.distro.code_correlation.internal.packages_resolver.sys.path", ["/path1", "/path2"]
        ):

            # Clear cache by modifying global variables
            import amazon.opentelemetry.distro.code_correlation.internal.packages_resolver as pkg_module

            pkg_module._sys_path_hash = None
            pkg_module._resolved_sys_path = []

            # First call
            result1 = _resolve_system_paths()

            # Second call should return cached result
            result2 = _resolve_system_paths()

            self.assertEqual(result1, result2)
            self.assertEqual(len(result1), 2)

    def test_resolve_system_paths_cache_invalidation(self):
        """Test that cache is invalidated when sys.path changes."""
        import amazon.opentelemetry.distro.code_correlation.internal.packages_resolver as pkg_module

        with patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver.sys.path", ["/path1"]):
            pkg_module._sys_path_hash = None  # Clear cache
            result1 = _resolve_system_paths()

        with patch(
            "amazon.opentelemetry.distro.code_correlation.internal.packages_resolver.sys.path", ["/path1", "/path2"]
        ):
            result2 = _resolve_system_paths()

        self.assertNotEqual(len(result1), len(result2))


class TestExtractRootModuleName(TestCase):
    """Test the _extract_root_module_name function."""

    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._PURELIB_PATH")
    def test_extract_root_module_name_purelib(self, mock_purelib):
        """Test extracting root module name from purelib path."""
        mock_purelib.return_value = Path("/usr/lib/python3.9/site-packages")

        with patch.object(Path, "resolve") as mock_resolve, patch.object(
            Path, "relative_to"
        ) as mock_relative_to, patch(
            "amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._determine_effective_root"
        ) as mock_determine:

            mock_resolve.return_value = Path("/usr/lib/python3.9/site-packages/mypackage/module.py")
            mock_relative_to.return_value = Path("mypackage/module.py")
            mock_determine.return_value = "mypackage"

            file_path = Path("/usr/lib/python3.9/site-packages/mypackage/module.py")
            result = _extract_root_module_name(file_path)

            self.assertEqual(result, "mypackage")

    def test_extract_root_module_name_value_error(self):
        """Test _extract_root_module_name raises ValueError when module cannot be determined."""
        with patch.object(Path, "relative_to", side_effect=ValueError("not relative")), patch(
            "amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._resolve_system_paths",
            return_value=[],
        ):

            file_path = Path("/unknown/path/module.py")

            with self.assertRaises(ValueError) as context:
                _extract_root_module_name(file_path)

            self.assertIn("Could not determine root module", str(context.exception))


class TestBuildPackageMapping(TestCase):
    """Test the _build_package_mapping function."""

    @patch("importlib.metadata.distributions")
    def test_build_package_mapping_success(self, mock_distributions):
        """Test successful package mapping build."""
        # Mock distribution
        mock_dist = Mock()
        mock_dist.metadata = {"name": "test-package", "version": "1.0.0"}
        mock_dist.files = [
            Mock(parts=["testpkg", "module.py"], locate=lambda: Path("/site-packages/testpkg/module.py"))
        ]

        mock_distributions.return_value = [mock_dist]

        # Clear the cache by calling the function directly
        result = _build_package_mapping.__wrapped__()

        self.assertIsInstance(result, dict)
        self.assertIn("testpkg", result)
        self.assertEqual(result["testpkg"].name, "test-package")
        self.assertEqual(result["testpkg"].version, "1.0.0")

    @patch("importlib.metadata.distributions")
    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._logger")
    def test_build_package_mapping_exception(self, mock_logger, mock_distributions):
        """Test package mapping build handles exceptions."""
        mock_distributions.side_effect = Exception("Import error")

        result = _build_package_mapping.__wrapped__()

        self.assertIsNone(result)
        mock_logger.warning.assert_called_once()

    @patch("importlib.metadata.distributions")
    def test_build_package_mapping_namespace_packages(self, mock_distributions):
        """Test package mapping handles namespace packages."""
        # Mock distribution with namespace package
        mock_dist = Mock()
        mock_dist.metadata = {"name": "namespace-pkg", "version": "1.0.0"}

        # Mock file path that represents a namespace package
        mock_file = Mock()
        mock_file.parts = ["namespace", "subpkg", "module.py"]
        mock_file.locate.return_value = Path("/site-packages/namespace/subpkg/module.py")

        mock_dist.files = [mock_file]
        mock_distributions.return_value = [mock_dist]

        # Mock Path methods to simulate namespace package (no __init__.py)
        with patch.object(Path, "is_dir", return_value=True), patch.object(
            Path, "exists", return_value=False
        ):  # No __init__.py

            result = _build_package_mapping.__wrapped__()

            # Should create mapping for namespace/subpkg
            self.assertIn("namespace/subpkg", result)

    @patch("importlib.metadata.distributions")
    def test_build_package_mapping_skip_metadata(self, mock_distributions):
        """Test package mapping skips metadata directories."""
        mock_dist = Mock()
        mock_dist.metadata = {"name": "test-pkg", "version": "1.0.0"}
        mock_dist.files = [
            Mock(parts=["test_pkg-1.0.0.dist-info", "METADATA"]),
            Mock(parts=["test_pkg-1.0.0.egg-info", "PKG-INFO"]),
            Mock(parts=["..", "something"]),
            Mock(parts=["testpkg", "module.py"], locate=lambda: Path("/site-packages/testpkg/module.py")),
        ]

        mock_distributions.return_value = [mock_dist]

        result = _build_package_mapping.__wrapped__()

        # Should only have the actual package, not metadata directories
        self.assertEqual(len(result), 1)
        self.assertIn("testpkg", result)


class TestLoadThirdPartyPackages(TestCase):
    """Test the _load_third_party_packages function."""

    @patch("importlib.resources.read_text")
    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._code_attributes_config")
    def test_load_third_party_packages_success(self, mock_config, mock_read_text):
        """Test successful loading of third-party packages."""
        # Mock text file content
        mock_read_text.return_value = "package1\npackage2\npackage3"

        # Mock configuration
        mock_config.include = ["extra_package"]
        mock_config.exclude = ["package2"]

        result = _load_third_party_packages.__wrapped__()

        expected = {"package1", "package3", "extra_package"}  # package2 excluded
        self.assertEqual(result, expected)

    @patch("importlib.resources.read_text")
    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._logger")
    def test_load_third_party_packages_exception(self, mock_logger, mock_read_text):
        """Test loading third-party packages handles exceptions."""
        mock_read_text.side_effect = Exception("Read error")

        result = _load_third_party_packages.__wrapped__()

        self.assertEqual(result, set())
        mock_logger.warning.assert_called_once()


class TestResolvePackageFromFilename(TestCase):
    """Test the resolve_package_from_filename function."""

    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._build_package_mapping")
    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._extract_root_module_name")
    def test_resolve_package_from_filename_exact_match(self, mock_extract, mock_build):
        """Test resolving package with exact module name match."""
        # Mock package mapping
        test_dist = Distribution(name="test-package", version="1.0.0")
        mock_build.return_value = {"testpkg": test_dist}
        mock_extract.return_value = "testpkg"

        result = resolve_package_from_filename("/path/to/testpkg/module.py")

        self.assertEqual(result, test_dist)

    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._build_package_mapping")
    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._extract_root_module_name")
    def test_resolve_package_from_filename_name_match(self, mock_extract, mock_build):
        """Test resolving package with distribution name match."""
        test_dist = Distribution(name="test-package", version="1.0.0")
        mock_build.return_value = {"otherpkg": test_dist}
        mock_extract.return_value = "test-package"  # Matches distribution name

        result = resolve_package_from_filename("/path/to/test-package/module.py")

        self.assertEqual(result, test_dist)

    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._build_package_mapping")
    def test_resolve_package_from_filename_no_mapping(self, mock_build):
        """Test resolving package when no mapping is available."""
        mock_build.return_value = None

        result = resolve_package_from_filename("/path/to/unknown/module.py")

        self.assertIsNone(result)

    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._build_package_mapping")
    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._extract_root_module_name")
    def test_resolve_package_from_filename_no_match(self, mock_extract, mock_build):
        """Test resolving package when no match is found."""
        mock_build.return_value = {"otherpkg": Distribution("other", "1.0")}
        mock_extract.return_value = "unknown"

        result = resolve_package_from_filename("/path/to/unknown/module.py")

        self.assertIsNone(result)

    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._build_package_mapping")
    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._extract_root_module_name")
    def test_resolve_package_from_filename_path_object(self, mock_extract, mock_build):
        """Test resolving package with Path object input."""
        test_dist = Distribution(name="test-package", version="1.0.0")
        mock_build.return_value = {"testpkg": test_dist}
        mock_extract.return_value = "testpkg"

        path_obj = Path("/path/to/testpkg/module.py")
        result = resolve_package_from_filename(path_obj)

        self.assertEqual(result, test_dist)

    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._build_package_mapping")
    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._extract_root_module_name")
    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._logger")
    def test_resolve_package_from_filename_exception(self, mock_logger, mock_extract, mock_build):
        """Test resolving package handles exceptions."""
        mock_build.return_value = {"testpkg": Distribution("test", "1.0")}
        mock_extract.side_effect = ValueError("Extract error")

        # Clear the LRU cache for this test
        resolve_package_from_filename.cache_clear()

        result = resolve_package_from_filename("/path/to/unknown/module.py")

        self.assertIsNone(result)
        mock_logger.debug.assert_called()


class TestIsStandardLibrary(TestCase):
    """Test the is_standard_library function."""

    def test_is_standard_library_site_packages(self):
        """Test detection of site-packages (not stdlib)."""
        purelib_path = Path(sysconfig.get_path("purelib"))
        test_path = purelib_path / "requests" / "__init__.py"

        with patch.object(Path, "is_relative_to") as mock_is_relative:
            # Mock to return False for stdlib, True for site-packages
            def side_effect(path):
                return path == purelib_path

            mock_is_relative.side_effect = side_effect

            result = is_standard_library(test_path)
            self.assertFalse(result)

    def test_is_standard_library_symlink_resolution(self):
        """Test standard library detection resolves symlinks."""
        test_path = Path("/some/symlink/os.py")

        with patch.object(Path, "is_absolute", return_value=False), patch.object(
            Path, "is_symlink", return_value=True
        ), patch.object(Path, "resolve") as mock_resolve:

            mock_resolve.return_value = Path(sysconfig.get_path("stdlib")) / "os.py"

            # The function should call resolve() and then check the resolved path
            is_standard_library(test_path)
            mock_resolve.assert_called_once()


class TestIsThirdPartyPackage(TestCase):
    """Test the is_third_party_package function."""

    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver.resolve_package_from_filename")
    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._load_third_party_packages")
    def test_is_third_party_package_true(self, mock_load_packages, mock_resolve):
        """Test detection of third-party package."""
        mock_resolve.return_value = Distribution(name="requests", version="2.25.1")
        mock_load_packages.return_value = {"requests", "urllib3"}

        result = is_third_party_package(Path("/path/to/requests/__init__.py"))

        self.assertTrue(result)

    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver.resolve_package_from_filename")
    def test_is_third_party_package_no_distribution(self, mock_resolve):
        """Test detection when no distribution is found."""
        mock_resolve.return_value = None

        result = is_third_party_package(Path("/path/to/unknown/__init__.py"))

        self.assertFalse(result)

    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver.resolve_package_from_filename")
    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._load_third_party_packages")
    def test_is_third_party_package_not_in_list(self, mock_load_packages, mock_resolve):
        """Test detection when package is not in third-party list."""
        mock_resolve.return_value = Distribution(name="myapp", version="1.0.0")
        mock_load_packages.return_value = {"requests", "urllib3"}

        result = is_third_party_package(Path("/path/to/myapp/__init__.py"))

        self.assertFalse(result)


class TestIsUserCode(TestCase):
    """Test the is_user_code function."""

    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver.is_standard_library")
    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver.is_third_party_package")
    def test_is_user_code_true(self, mock_is_third_party, mock_is_stdlib):
        """Test detection of user code."""
        mock_is_stdlib.return_value = False
        mock_is_third_party.return_value = False

        result = is_user_code("/path/to/myapp/module.py")

        self.assertTrue(result)

    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver.is_standard_library")
    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver.is_third_party_package")
    def test_is_user_code_stdlib(self, mock_is_third_party, mock_is_stdlib):
        """Test detection when file is standard library."""
        mock_is_stdlib.return_value = True
        mock_is_third_party.return_value = False

        result = is_user_code("/usr/lib/python3.9/os.py")

        self.assertFalse(result)

    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver.is_standard_library")
    @patch("amazon.opentelemetry.distro.code_correlation.internal.packages_resolver.is_third_party_package")
    def test_is_user_code_third_party(self, mock_is_third_party, mock_is_stdlib):
        """Test detection when file is third-party package."""
        mock_is_stdlib.return_value = False
        mock_is_third_party.return_value = True

        result = is_user_code("/path/to/site-packages/requests/__init__.py")

        self.assertFalse(result)


class TestPackagesIntegration(TestCase):
    """Integration tests for packages module."""

    def test_distribution_in_resolve_package_workflow(self):
        """Test Distribution is properly used in the resolution workflow."""
        with patch(
            "amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._build_package_mapping"
        ) as mock_build, patch(
            "amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._extract_root_module_name"
        ) as mock_extract:

            test_dist = Distribution(name="test-pkg", version="1.2.3")
            mock_build.return_value = {"testpkg": test_dist}
            mock_extract.return_value = "testpkg"

            result = resolve_package_from_filename("/path/to/testpkg/module.py")

            self.assertIsInstance(result, Distribution)
            self.assertEqual(result.name, "test-pkg")
            self.assertEqual(result.version, "1.2.3")

    def test_caching_behavior(self):
        """Test that caching functions work correctly."""
        # Test that resolve_package_from_filename uses caching
        with patch(
            "amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._build_package_mapping"
        ) as mock_build, patch(
            "amazon.opentelemetry.distro.code_correlation.internal.packages_resolver._extract_root_module_name"
        ) as mock_extract:

            test_dist = Distribution(name="cached-pkg", version="1.0.0")
            mock_build.return_value = {"cachedpkg": test_dist}
            mock_extract.return_value = "cachedpkg"

            # First call
            result1 = resolve_package_from_filename("/path/to/cachedpkg/module.py")

            # Second call with same path should use cache
            result2 = resolve_package_from_filename("/path/to/cachedpkg/module.py")

            self.assertEqual(result1, result2)
            self.assertEqual(result1.name, "cached-pkg")

            # Should have been called only once due to LRU cache
            self.assertEqual(mock_extract.call_count, 1)

    def test_module_constants(self):
        """Test that module constants are properly initialized."""
        from amazon.opentelemetry.distro.code_correlation.internal.packages_resolver import (
            _PLATLIB_PATH,
            _PLATSTDLIB_PATH,
            _PURELIB_PATH,
            _STDLIB_PATH,
        )

        # All paths should be Path objects
        self.assertIsInstance(_STDLIB_PATH, Path)
        self.assertIsInstance(_PLATSTDLIB_PATH, Path)
        self.assertIsInstance(_PURELIB_PATH, Path)
        self.assertIsInstance(_PLATLIB_PATH, Path)

        # All paths should be absolute and resolved
        self.assertTrue(_STDLIB_PATH.is_absolute())
        self.assertTrue(_PLATSTDLIB_PATH.is_absolute())
        self.assertTrue(_PURELIB_PATH.is_absolute())
        self.assertTrue(_PLATLIB_PATH.is_absolute())
