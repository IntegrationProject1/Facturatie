<?php

if (empty($_SERVER['HTTPS'])) {
    header('Location: https://' . $_SERVER['HTTP_HOST'] . $_SERVER['REQUEST_URI']);
    exit();
}

$cfg["ForceSSL"] = true;
$cfg["Servers"][1]["ssl"] = true;
$cfg["Servers"][1]["ssl_key"] = "/etc/ssl/private/key.pem";
$cfg["Servers"][1]["ssl_cert"] = "/etc/ssl/certs/cert.pem";