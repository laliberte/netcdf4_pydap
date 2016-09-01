"""
Based on netcdf4-python

This code aims to provide:

A (partial) compatibility layer with netcdf4-python. In order to do so code was directly
borrowed from netcdf4-python package.

Frederic Laliberte,2016

with special thanks to
Jeff Whitaker and co-contributors (netcdf4-python)
"""

import re
from urlparse import urlsplit, urlunsplit

import requests
import warnings

import os
import datetime
import numpy as np

from collections import OrderedDict

import netCDF4.utils as utils
from pydap.exceptions import ServerError

#Internal:
from . import requests_http

python3=False
default_encoding = 'utf-8'

class Dataset:
    def __init__(self,url,cache=None,expire_after=datetime.timedelta(hours=1),timeout=120,
                          session=None,username=None,password=None,
                          authentication_url='ESGF', use_certificates=False):
        self._url = url
        self._pydap_instance = requests_http.Pydap_Dataset(self._url, cache=cache, expire_after=expire_after,
                                                            timeout=timeout, session=session, 
                                                            username=username, password=password, 
                                                            authentication_url=authentication_url,
                                                            use_certificates=use_certificates)

        #Provided for compatibility:
        self.data_model = 'pyDAP'
        self.file_format = self.data_model
        self.disk_format = 'DAP2'
        self._isopen = 1
        self.path = '/'
        self.parent = None
        self.keepweakref = False

        self.dimensions = self._get_dims(self._pydap_instance._dataset)
        self.variables = self._get_vars(self._pydap_instance._dataset)

        self.groups = OrderedDict()
        return

    def __enter__(self):
        return self

    def __exit__(self,atype,value,traceback):
        self.close()
        return

    def __getitem__(self, elem):
        #There are no groups. Simple mapping to variable:
        if elem in self.variables.keys():
            return self.variables[elem]
        else:
            raise IndexError('%s not found in %s' % (lastname,group.path))

    def filepath(self):
        return self._url

    def __repr__(self):
        if python3:
            return self.__unicode__()
        else:
            return unicode(self).encode(default_encoding)

    def __unicode__(self):
        #taken directly from netcdf4-python netCDF4.pyx
        ncdump = ['%r\n' % type(self)]
        dimnames = tuple([utils._tostr(dimname)+'(%s)'%len(self.dimensions[dimname])\
        for dimname in self.dimensions.keys()])
        varnames = tuple(\
        [utils._tostr(self.variables[varname].dtype)+' \033[4m'+utils._tostr(varname)+'\033[0m'+
        (((utils._tostr(self.variables[varname].dimensions)
        .replace("u'",""))\
        .replace("'",""))\
        .replace(", ",","))\
        .replace(",)",")") for varname in self.variables.keys()])
        grpnames = tuple([utils._tostr(grpname) for grpname in self.groups.keys()])
        if self.path == '/':
            ncdump.append('root group (%s data model, file format %s):\n' %
                    (self.data_model, self.disk_format))
        else:
            ncdump.append('group %s:\n' % self.path)
        attrs = ['    %s: %s\n' % (name,self.getncattr(name)) for name in\
                self.ncattrs()]
        ncdump = ncdump + attrs
        ncdump.append('    dimensions(sizes): %s\n' % ', '.join(dimnames))
        ncdump.append('    variables(dimensions): %s\n' % ', '.join(varnames))
        ncdump.append('    groups: %s\n' % ', '.join(grpnames))
        return ''.join(ncdump)

    def close(self):
        self._pydap_instance.close()
        self._isopen=0
        return

    def isopen(self):
        return bool(self._isopen)

    def ncattrs(self):
        try:
            return self._pydap_instance._dataset.attributes['NC_GLOBAL'].keys()
        except KeyError as e:
            return []

    def getncattr(self,attr):
        return self._pydap_instance._dataset.attributes['NC_GLOBAL'][attr]

    def __getattr__(self,name):
        #from netcdf4-python
        # if name in _private_atts, it is stored at the python
        # level and not in the netCDF file.
        if name.startswith('__') and name.endswith('__'):
            # if __dict__ requested, return a dict with netCDF attributes.
            if name == '__dict__':
                names = self.ncattrs()
                values = []
                for name in names:
                    values.append(self._pydap_instance._dataset.attributes['NC_GLOBAL'][attr])
                return OrderedDict(zip(names,values))
            else:
                raise AttributeError
        else:
            return self.getncattr(name)

    def set_auto_maskandscale(self,flag):
        raise NotImplementedError('set_auto_maskandscale is not implemented for pydap')
        return

    def set_auto_mask(self,flag):
        raise NotImplementedError('set_auto_mask is not implemented for pydap')
        return

    def set_auto_scale(self,flag):
        raise NotImplementedError('set_auto_scale is not implemented for pydap')
        return

    def get_variables_by_attributes(self,**kwargs):
        #From netcdf4-python
        vs = []

        r
        has_value_flag  = False
        for vname in self.variables:
            var = self.variables[vname]
            for k, v in kwargs.items():
                if callable(v):
                    has_value_flag = v(getattr(var, k, None))
                    if has_value_flag is False:
                        break
                #elif hasattr(var, k) and getattr(var, k) == v:
                #Must use getncattr
                elif hasattr(var, k) and var.getncattr(k) == v:
                    has_value_flag = True
                else:
                    has_value_flag = False
                    break
            if has_value_flag is True:
                vs.append(self.variables[vname])
        return vs

    def _get_dims(self, dataset):
        if ('DODS_EXTRA' in dataset.attributes.keys() and
            'Unlimited_Dimension' in dataset.attributes['DODS_EXTRA']):
            unlimited_dims = [dataset.attributes['DODS_EXTRA']['Unlimited_Dimension'],]
        else:
            unlimited_dims = []
        var_list = dataset.keys()
        var_id = np.argmax(map(len,[dataset[varname].dimensions for varname in var_list]))
        base_dimensions_list = dataset[var_list[var_id]].dimensions
        base_dimensions_lengths = dataset[var_list[var_id]].shape
        
        for varname in var_list:
            if not set(base_dimensions_list).issuperset(dataset[varname].dimensions):
                for dim_id,dim in enumerate(dataset[varname].dimensions):
                    if not dim in base_dimensions_list:
                        base_dimensions_list += (dim,)
                        base_dimensions_lengths += (dataset[varname].shape[dim_id],)
        dimensions_dict = OrderedDict()
        for dim,dim_length in zip( base_dimensions_list,base_dimensions_lengths):
            dimensions_dict[dim] = Dimension(dataset,dim,size=dim_length,isunlimited=(dim in unlimited_dims))
        return  dimensions_dict

    def _get_vars(self, dataset):
        return {var:Variable(dataset[var],var,self) for var in dataset.keys()}

class Variable:
    def __init__(self,var,name,grp):
        self._grp = grp
        self._var = var
        self.dimensions = self._getdims()
        if self._var.type.descriptor == 'String':
            self.dtype = np.dtype('S1')
        else:
            self.dtype = np.dtype(self._var.type.typecode)
        self.datatype = self.dtype
        self.ndim = len(self.dimensions)
        self.shape = self._var.shape
        self.scale = True
        self.name = name
        self.size = np.prod(self.shape)
        return

    def chunking(self):
        return 'contiguous'

    def filters(self):
        return None

    def get_var_chunk_cache(self):
        raise NotImplementedError('get_var_chunk_cache is not implemented')
        return

    def ncattrs(self):
        return self._var.attributes.keys()

    def getncattr(self,attr):
        return self._var.attributes[attr]

    def get_var_chunk_cache(self):
        raise NotImpletedError('get_var_chunk_cache is not implemented for pydap')

    def __getattr__(self,name):
        #from netcdf4-python
        # if name in _private_atts, it is stored at the python
        # level and not in the netCDF file.
        if name.startswith('__') and name.endswith('__'):
            # if __dict__ requested, return a dict with netCDF attributes.
            if name == '__dict__':
                names = self.ncattrs()
                values = []
                for name in names:
                    values.append(self._var.attributes[attr])
                return OrderedDict(zip(names,values))
            else:
                raise AttributeError
        else:
            return self.getncattr(name)

    def getValue(self):
        return self._var[...]

    def group(self):
        return self._grp

    def __array__(self):
        return self[...]

    def __repr__(self):
        if python3:
            return self.__unicode__()
        else:
            return unicode(self).encode(default_encoding)

    def __getitem__(self,getitem_tuple):
        try:
            return self._var.array.__getitem__(getitem_tuple)
        except (AttributeError, ServerError,requests.exceptions.HTTPError) as e:
            if ( 
                 isinstance(getitem_tuple,slice) and
                 getitem_tuple == _PhonyVariable()[:]):
                #A single dimension ellipsis was requested. Use netCDF4 convention:
                return self[...]
            else:
                return self._var.__getitem__(getitem_tuple)

    def __len__(self):
        if not self.shape:
            raise TypeError('len() of unsized object')
        else:
            return self.shape[0]

    def set_auto_maskandscale(self,maskandscale):
        raise NotImplementedError('set_auto_maskandscale is not implemented for pydap')

    def set_auto_scale(self,scale):
        raise NotImplementedError('set_auto_scale is not implemented for pydap')

    def set_auto_mask(self,mask):
        raise NotImplementedError('set_auto_mask is not implemented for pydap')

    def __unicode__(self):
        #taken directly from netcdf4-python: netCDF4.pyx
        if not dir(self._grp._pydap_instance._dataset):
            return 'Variable object no longer valid'
        ncdump_var = ['%r\n' % type(self)]
        dimnames = tuple([utils._tostr(dimname) for dimname in self.dimensions])
        attrs = ['    %s: %s\n' % (name,self.getncattr(name)) for name in\
                self.ncattrs()]
        ncdump_var.append('%s %s(%s)\n' %\
        (self.dtype, self.name, ', '.join(dimnames)))
        ncdump_var = ncdump_var + attrs
        unlimdims = []
        for dimname in self.dimensions:
            dim = self._grp.dimensions[dimname]
            if dim.isunlimited():
                unlimdims.append(dimname)
        ncdump_var.append('unlimited dimensions: %s\n' % ', '.join(unlimdims))
        ncdump_var.append('current shape = %s\n' % repr(self.shape))
        no_fill=0
        return ''.join(ncdump_var)

    def _getdims(self):
        return self._var.dimensions

class Dimension:
    def __init__(self, grp, name, size=0, isunlimited=True):
        self._grp = grp

        self.size = size
        self._isunlimited = isunlimited

        self._name = name
        #self._data_model=self._grp.data_model

    def __len__(self):
        return self.size

    def isunlimited(self):
        return self._isunlimited

    def group(self):
        return self._grp

    def __repr__(self):
        if python3:
            return self.__unicode__()
        else:
            return unicode(self).encode(default_encoding)

    def __unicode__(self):
        #taken directly from netcdf4-python: netCDF4.pyx
        if not dir(self._grp):
            return 'Dimension object no longer valid'
        if self.isunlimited():
            return repr(type(self))+" (unlimited): name = '%s', size = %s\n" % (self._name,len(self))
        else:
            return repr(type(self))+": name = '%s', size = %s\n" % (self._name,len(self))


class _PhonyVariable:
    #A phony variable to translate getitems:
    def __init__(self):
        pass

    def __getitem__(self, getitem_tuple):
        return getitem_tuple
