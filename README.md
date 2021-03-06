# moop-cull-service

Extended 'cull idle server' service for jupyterhub, to cull idle servers with culling record support.  

[![LICENSE](https://img.shields.io/badge/license-Anti%20996-blue.svg)](https://github.com/996icu/996.ICU/blob/master/LICENSE)

## environment

```sh
conda install python-dateutil tornado pycurl pyyaml -c conda-forge
```

## jupyterhub config

add the following service code to your jupyterhub ```config.yaml```:  

```yaml
hub:
  services:
    cull:
      admin: true,
      api_token: 'ad6b8dc16f624b54a5b7d265f0744c97' # API Token，要与cull-service的配置对应
```

## config.yaml

Please place config.yaml under the root of cull service.  

config.yaml:  

```yaml
debug: true
# 10 - debug
log_level: 10
jupyterhub_url: 'http://192.168.0.31:30264'
jupyterhub_api_prefix: '/hub/api'
jupyterhub_api_token: 'ad6b8dc16f624b54a5b7d265f0744c97'
# moop-es-service url (NOT ES)
es_service_url: 'http://192.168.0.31:31786'
# tenant ID from tenant service!
tenant: '5cc026228c74b2d34997744d'
# server will be culled being inactive for 30 mins
timeout: 1800
# scan servers every 2 mins
cull_interval: 180
# server will run 4 hours max
max_age: 14400
# we don't cull idle users
cull_users: false
# do not tune this
concurrency: 10
```

## dev start

```sh
python cull-service.py
```

## API

No API needed.  
