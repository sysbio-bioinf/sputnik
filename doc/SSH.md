# Key-based SSH Authentication

Key-based SSH authentication is needed for deployment of Sputnik server and workers.
This document explains the setup on Linux machines. The general steps are similar on other operating systems (probably other tools are used).

## Private key generation

In case you do not have a private key for SSH, you can generate one with key size 2048 bit as follows.
```bash
$ ssh-keygen -b 2048

Generating public/private rsa key pair.
Enter file in which to save the key (/home/youruser/.ssh/id_rsa):
```
When prompted for the location, just accept the default location.
It is recommended to choose a password for the key.


## Register public key remotely

To enable key-based authentication you must register the public key (corresponding to the private key generated before) on the remote node.
The easiest method to do that is shown below.
```bash
$ ssh-copy-id username@mynode.mydomain.org
```

## Load private key

To use the deployment of Sputnik you need to load your private key.
Provided the private key is at the default location it can be loaded via ```ssh-add```.
Otherwise, the location of the key needs to be added, e.g. ```ssh-add /home/youruser/keys/id_rsa```.

In case ```ssh-add``` fails, you might need to start ssh-agent manually
```bash
$ ssh-agent
```
and export the environment variables as it tells you, e.g.
```bash
$ SSH_AUTH_SOCK=/tmp/ssh-y9kNxa8k4BJB/agent.2922; export SSH_AUTH_SOCK;
$ SSH_AGENT_PID=2923; export SSH_AGENT_PID;
```
