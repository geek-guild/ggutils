# ggutils

  - Python Utilities by Geek Guild

## Install

```
$ pip install git+https://github.com/geek-guild/ggutils.git
```

## Install

```
$ pip install git+https://github.com/geek-guild/ggutils.git
```

### Install past or future release versions
```
$ pip install git+https://github.com/geek-guild/ggutils.git@release/v0.0.4
```

## usage

```
$ python
>>> import ggutils.examples.getting_started as getting_started
>>> getting_started.hello()
'Hello, World! This is module:[$VENV_PATH/lib/python3.7/site-packages/ggutils/examples/getting_started.py] function:[hello].'
```

# Direct install from local clone
```
rsync -avz -e "ssh -i $YOUR_SECRET_PEM_PATH" ~/github/geek-guild/ggutils/ggutils/ $USER_NAME@$INS_IP:/home/$USER_NAME/github/geek-guild/ggutils/ggutils/
rm -Rf $VENV_PATH/lib/python3.7/site-packages/ggutils/
cp -Rf ~/github/geek-guild/ggutils/ggutils/ $VENV_PATH/lib/python3.7/site-packages/ggutils/
```
