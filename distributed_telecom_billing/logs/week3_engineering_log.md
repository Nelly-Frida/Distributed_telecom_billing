# Week 3 Failure-Driven Engineering Log

## What Changed?
Introduced Apache HTTP Server as frontend, plus a Python HTTP API
(api/billing_api.py) that proxies to Node 2. Node 1 can now talk HTTP
through Apache. MetricsLogger writes CSVs for Power BI.

## What Failed?
Apache returned 502 Bad Gateway on /api/bill.

## Why Did It Fail?
mod_proxy and mod_proxy_http were not loaded in the default XAMPP config,
so ProxyPass had no effect.

## How Was It Fixed?
Uncommented LoadModule proxy_module and LoadModule proxy_http_module in
apache/httpd.conf and reloaded Apache.

## Alternative Considered
Bypass Apache and have Power BI read CSVs from disk. Rejected because a
real gateway must sit behind an HTTP frontend for authentication later.

## What Was Learned?
A reverse proxy is not transparent until its target modules are loaded.
The API contract must be settled BEFORE writing the Apache config.
