# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi

# User specific aliases and functions
export GOPATH=/net/20/kun/go/path
export GOROOT=/net/20/kun/go
export PYTHONPATH=/net/20/kun/python-2.7.13
export REDISPATH=/net/20/kun/redis-3.2.7
export MONGODBPATH=/net/20/kun/mongodb-linux-x86_64-3.4.2
export PATH=$REDISPATH/bin:$MONGODBPATH/bin:$PYTHONPATH/bin:~/.local/bin:$GOPATH/bin:$GOROOT/bin:$PATH
#export PATH=$REDISPATH/bin:$MONGODBPATH/bin:$PYTHONPATH/bin:~/.local/bin:$PATH
export CRYPTOGRAPHY_ALLOW_OPENSSL_100=1
