from __future__ import absolute_import

from functools import partial

try:
    from urllib.parse import parse_qsl, quote, unquote, urlparse
except ImportError:
    from urllib import quote, unquote                  # noqa
    from urlparse import urlparse, parse_qsl    # noqa

from . import kwdict
from kombu.five import string_t

safequote = partial(quote, safe='')


# https://github.com/celery/kombu/commit/cbf4fd6afd613da5ee9c8b50d5ad0d905563faeb
def _parse_url(url):
    scheme = urlparse(url).scheme
    schemeless = url[len(scheme) + 3:]
    # parse with HTTP URL semantics
    parts = urlparse("http://" + schemeless)

    # The first pymongo.Connection() argument (host) can be
    # a mongodb connection URI. If this is the case, don't
    # use port but let pymongo get the port(s) from the URI instead.
    # This enables the use of replica sets and sharding.
    # See pymongo.Connection() for more info.
    port = scheme != 'mongodb' and parts.port or None
    hostname = schemeless if scheme == 'mongodb' else parts.hostname
    path = parts.path or ''
    path = path[1:] if path and path[0] == '/' else path
    return (scheme, unquote(hostname or '') or None, port,
            unquote(parts.username or '') or None,
            unquote(parts.password or '') or None,
            unquote(path or '') or None,
            kwdict(dict(parse_qsl(parts.query))))


def parse_url(url):
    scheme, host, port, user, password, path, query = _parse_url(url)
    return dict(transport=scheme, hostname=host,
                port=port, userid=user,
                password=password, virtual_host=path, **query)


def as_url(scheme, host=None, port=None, user=None, password=None,
           path=None, query=None, sanitize=False, mask='**'):
        parts = ['{0}://'.format(scheme)]
        if user or password:
            if user:
                parts.append(safequote(user))
            if password:
                if sanitize:
                    parts.extend([':', mask] if mask else [':'])
                else:
                    parts.extend([':', safequote(password)])
            parts.append('@')
        parts.append(safequote(host) if host else '')
        if port:
            parts.extend([':', port])
        parts.extend(['/', path])
        return ''.join(str(part) for part in parts if part)


def sanitize_url(url, mask='**'):
    return as_url(*_parse_url(url), sanitize=True, mask=mask)


def maybe_sanitize_url(url, mask='**'):
    if isinstance(url, string_t) and '://' in url:
        return sanitize_url(url, mask)
    return url
