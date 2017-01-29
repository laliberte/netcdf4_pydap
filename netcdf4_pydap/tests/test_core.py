"""
    Test the DAP handler, which forms the core of the client.
"""

import netCDF4
import numpy as np
from six.moves import zip
from webob.exc import HTTPError
from ssl import SSLError

from netcdf4_pydap.pydap_fork.pydap.src\
     .pydap.handlers.netcdf import NetCDFHandler
from netcdf4_pydap.pydap_fork.pydap.src\
     .pydap.wsgi.ssf import ServerSideFunctions
from netcdf4_pydap.pydap_fork.pydap.src\
     .pydap.exceptions import ServerError
from netcdf4_pydap import Dataset

import pytest


class MockErrors:
    def __init__(self, errors):
        self.errors = errors
        self.error_id = 0

    def __call__(self, *args, **kwargs):
        self.error_id += 1
        raise self.errors[min(self.error_id - 1,
                              len(self.errors) - 1)]


def _message(e):
    try:
        return e.exception.message
    except AttributeError:
        try:
            return str(e.exception)
        except AttributeError:
            return str(e.value)


@pytest.fixture(scope='module')
def data():
    data = [(10, 15.2, 'Diamond_St'),
            (11, 13.1, 'Blacktail_Loop'),
            (12, 13.3, 'Platinum_St'),
            (13, 12.1, 'Kodiak_Trail')]
    return data


@pytest.fixture(scope='module')
def app(tmpdir_factory, data):
    """Create WSGI apps"""

    # Create tempfile:
    test_file = str(tmpdir_factory.mktemp('data').join('test.nc'))
    with netCDF4.Dataset(test_file, 'w') as output:
        output.createDimension('index', None)
        temp = output.createVariable('index', '<i4', ('index',))
        split_data = zip(*data)
        temp[:] = next(split_data)
        temp = output.createVariable('temperature', '<f8', ('index',))
        temp[:] = next(split_data)
        temp = output.createVariable('station', 'S40', ('index',))
        temp.setncattr('long_name', 'Station Name')
        for item_id, item in enumerate(next(split_data)):
            temp[item_id] = item
        output.createDimension('tag', 1)
        temp = output.createVariable('tag', '<i4', ('tag',))
        output.setncattr('history', 'test file for netCDF4 api')
    return ServerSideFunctions(NetCDFHandler(test_file))


def test_dataset_direct(app, data):
    """Test that dataset has the correct data proxies for grids."""
    dtype = [('index', '<i4'),
             ('temperature', '<f8'),
             ('station', 'S40')]
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        retrieved_data = list(zip(dataset['index'][:],
                                  dataset['temperature'][:],
                                  dataset['station'][:]))
    np.testing.assert_array_equal(np.array(retrieved_data, dtype=dtype),
                                  np.array(data, dtype=dtype))


def test_dataset_missing_elem(app):
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        with pytest.raises(IndexError) as e:
            dataset['missing']
        assert str(_message(e)) == 'missing not found in /'


def test_dataset_httperror(app):

    mock_httperror = MockErrors([HTTPError('400 Test Error')])

    with pytest.raises(ServerError) as e:
        Dataset('http://localhost:8000/',
                application=mock_httperror)
    assert str(_message(e)) == "'400 Test Error'"

    mock_httperror = MockErrors([HTTPError('500 Test Error')])

    with pytest.raises(ServerError) as e:
        Dataset('http://localhost:8000/',
                application=mock_httperror)
    assert str(_message(e)) == "'500 Test Error'"


def test_dataset_sslerror(app):

    mock_sslerror = MockErrors([SSLError('SSL Test Error')])

    with pytest.raises(SSLError) as e:
        Dataset('http://localhost:8000/',
                application=mock_sslerror)
    assert str(_message(e)) == "('SSL Test Error',)"


def test_variable_httperror(app):
    mock_httperror = MockErrors([HTTPError('400 Test Error'),
                                 HTTPError('401 Test Error')])

    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        variable = dataset.variables['temperature']
        variable._var.array.__getitem__ = mock_httperror
        variable._var.__getitem__ = mock_httperror
        with pytest.raises(ServerError) as e:
            variable[...]
        assert str(_message(e)) == "'401 Test Error'"

    mock_httperror = MockErrors([HTTPError('400 Test Error'),
                                 HTTPError('500 Test Error')])

    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        variable = dataset.variables['temperature']
        variable._var.array.__getitem__ = mock_httperror
        variable._var.__getitem__ = mock_httperror
        with pytest.raises(ServerError) as e:
            variable[...]
        assert str(_message(e)) == "'500 Test Error'"


def test_variable_sslerror(app):

    mock_sslerror = MockErrors([HTTPError('400 Test Error'),
                                SSLError('SSL Test Error'),
                                HTTPError('500 Test Error')])

    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        variable = dataset.variables['temperature']
        variable._var.array.__getitem__ = mock_sslerror
        variable._var.__getitem__ = mock_sslerror
        with pytest.raises(ServerError) as e:
            variable[...]
        assert str(_message(e)) == "'500 Test Error'"

    mock_sslerror = MockErrors([HTTPError('400 Test Error'),
                                SSLError('SSL Test Error'),
                                HTTPError('500 Test Error')])
    mock_assignerror = MockErrors([SSLError('SSL dataset Error')])

    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        dataset._assign_dataset = mock_assignerror
        variable = dataset.variables['temperature']
        variable._var.array.__getitem__ = mock_sslerror
        variable._var.__getitem__ = mock_sslerror
        with pytest.raises(SSLError) as e:
                variable[...]
        assert str(_message(e)) == "('SSL dataset Error',)"


def test_dataset_filepath(app):
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        assert dataset.filepath() == 'http://localhost:8000/'


def test_dataset_repr(app):
    expected_repr = """<class 'netcdf4_pydap.core.Dataset'>
root group (pyDAP data model, file format DAP2):
    history: test file for netCDF4 api
    dimensions(sizes): index(4), tag(1)
    variables(dimensions): >i4 \033[4mindex\033[0m(index), |S100 \033[4mstation\033[0m(index), >i4 \033[4mtag\033[0m(tag), >f8 \033[4mtemperature\033[0m(index)
    groups: 
"""
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        assert repr(dataset) == expected_repr
        dataset.path = 'test/'
        expected_repr = '\n'.join(
                              [line if line_id != 1 else 'group test/:'
                               for line_id, line
                               in enumerate(expected_repr.split('\n'))])
        assert repr(dataset) == expected_repr


def test_dataset_isopen(app):
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        assert dataset.isopen()


def test_dataset_ncattrs(app):
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        assert list(dataset.ncattrs()) == ['history']
        del dataset._pydap_dataset.attributes['NC_GLOBAL']
        assert list(dataset.ncattrs()) == []


def test_dataset_getattr(app):
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        assert dataset.getncattr('history') == 'test file for netCDF4 api'
        assert getattr(dataset, 'history') == 'test file for netCDF4 api'
        with pytest.raises(AttributeError) as e:
            getattr(dataset, 'inexistent')
        assert str(_message(e)) == "'inexistent'"


def test_dataset_set_auto_maskandscale(app):
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        with pytest.raises(NotImplementedError) as e:
            dataset.set_auto_maskandscale(True)
        assert str(_message(e)) == ('set_auto_maskandscale is not '
                                    'implemented for pydap')


def test_dataset_set_auto_mask(app):
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        with pytest.raises(NotImplementedError) as e:
            dataset.set_auto_mask(True)
        assert str(_message(e)) == ('set_auto_mask is not '
                                    'implemented for pydap')


def test_dataset_set_auto_scale(app):
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        with pytest.raises(NotImplementedError) as e:
            dataset.set_auto_scale(True)
        assert str(_message(e)) == ('set_auto_scale is not '
                                    'implemented for pydap')


def test_dataset_get_variable_by_attribute(app):
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        var = dataset.get_variables_by_attributes(**{'long_name':
                                                     'Station Name'})
        assert var == [dataset.variables['station']]

        def station(x):
            try:
                return 'Station' in x
            except TypeError:
                return False
        var = dataset.get_variables_by_attributes(**{'long_name':
                                                     station})
        assert var == [dataset.variables['station']]

        def inexistent(x):
            return False

        assert callable(inexistent)
        var = dataset.get_variables_by_attributes(**{'long_name':
                                                     inexistent})
        assert var == []


def test_dimension_unlimited(app):
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        assert not dataset.dimensions['index'].isunlimited()
        assert isinstance(dataset._pydap_dataset
                          .attributes['DODS_EXTRA'], dict)
        assert 'Unlimited_Dimension' not in (dataset
                                             ._pydap_dataset
                                             .attributes['DODS_EXTRA'])
        (dataset._pydap_dataset
         .attributes['DODS_EXTRA']
         .update({'Unlimited_Dimension': 'index'}))
        dataset.dimensions = dataset._get_dims(dataset._pydap_dataset)
        assert dataset.dimensions['index'].isunlimited()


def test_dimension_group(app):
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        assert dataset.dimensions['index'].group() == dataset


def test_dimension_repr(app):
    expected_repr = ("<class 'netcdf4_pydap.pydap_fork.pydap.src.pydap.apis.netCDF4.Dimension'>: "
                     "name = 'index', size = 4")
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        assert repr(dataset
                    .dimensions['index']).strip() == expected_repr


def test_dimension_unlimited_repr(app):
    expected_repr = ("<class 'netcdf4_pydap.pydap_fork.pydap.src.pydap.apis.netCDF4.Dimension'> (unlimited): "
                     "name = 'index', size = 4")
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        assert not dataset.dimensions['index'].isunlimited()
        assert isinstance(dataset._pydap_dataset
                          .attributes['DODS_EXTRA'], dict)
        assert 'Unlimited_Dimension' not in (dataset
                                             ._pydap_dataset
                                             .attributes['DODS_EXTRA'])
        (dataset._pydap_dataset
         .attributes['DODS_EXTRA']
         .update({'Unlimited_Dimension': 'index'}))
        dataset.dimensions = dataset._get_dims(dataset._pydap_dataset)
        assert dataset.dimensions['index'].isunlimited()
        assert repr(dataset
                    .dimensions['index']).strip() == expected_repr


def test_variable_group(app):
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        variable = dataset.variables['temperature']
        assert variable.group() == dataset


def test_variable_length(app):
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        variable = dataset.variables['temperature']
        assert len(variable) == 4

        def mock_shape(x):
            return None

        variable._get_array_att = mock_shape
        with pytest.raises(TypeError) as e:
            len(variable)
        assert str(_message(e)) == 'len() of unsized object'


def test_variable_repr(app):
    expected_repr = """<class 'netcdf4_pydap.pydap_fork.pydap.src.pydap.apis.netCDF4.Variable'>
|S100 station(index)
    long_name: Station Name
unlimited dimensions: 
current shape = (4,)
"""
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        variable = dataset.variables['station']
        assert repr(variable) == expected_repr

        # Mock unlimited dimension:
        assert not dataset.dimensions['index'].isunlimited()
        assert isinstance(dataset._pydap_dataset
                          .attributes['DODS_EXTRA'], dict)
        assert 'Unlimited_Dimension' not in (dataset
                                             ._pydap_dataset
                                             .attributes['DODS_EXTRA'])
        (dataset._pydap_dataset
         .attributes['DODS_EXTRA']
         .update({'Unlimited_Dimension': 'index'}))
        dataset.dimensions = dataset._get_dims(dataset._pydap_dataset)
        assert repr(variable) == (expected_repr
                                  .replace('unlimited dimensions: ',
                                           'unlimited dimensions: index'))


def test_variable_hdf5_properties(app):
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        variable = dataset.variables['temperature']
        assert variable.chunking() == 'contiguous'
        assert variable.filters() is None
        with pytest.raises(NotImplementedError) as e:
            variable.get_var_chunk_cache()
        assert str(_message(e)) == ('get_var_chunk_cache is not '
                                    'implemented')


def test_variable_set_auto_maskandscale(app):
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        with pytest.raises(NotImplementedError) as e:
            variable = dataset.variables['temperature']
            variable.set_auto_maskandscale(True)
        assert str(_message(e)) == ('set_auto_maskandscale is not '
                                    'implemented for pydap')


def test_variable_set_auto_mask(app):
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        with pytest.raises(NotImplementedError) as e:
            variable = dataset.variables['temperature']
            variable.set_auto_mask(True)
        assert str(_message(e)) == ('set_auto_mask is not '
                                    'implemented for pydap')


def test_variable_set_auto_scale(app):
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        with pytest.raises(NotImplementedError) as e:
            variable = dataset.variables['temperature']
            variable.set_auto_scale(True)
        assert str(_message(e)) == ('set_auto_scale is not '
                                    'implemented for pydap')


def test_variable_get(app):
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        variable = dataset.variables['temperature']
        assert np.all(variable[:] == variable[...])
        assert np.all(variable[:] == np.asarray(variable))
        assert np.all(variable[:] == variable.getValue())


def test_variable_string_dtype(app):
    with Dataset('http://localhost:8000/',
                 application=app) as dataset:
        variable = dataset.variables['station']
        assert variable.dtype != 'S40'
        assert 'DODS' not in variable._var.attributes
        variable._var.attributes['DODS'] = {'dimName': 'string',
                                            'string': 40}
        assert variable.dtype == 'S40'
