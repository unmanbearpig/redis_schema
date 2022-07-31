use redis::{Cmd, ToRedisArgs};

pub trait Key<K: ToRedisArgs> {
    fn new(key: K) -> Self;

    fn key(self) -> K;

    fn del(self) -> Cmd
        where Self: Sized
    {
        Cmd::del(self.key())
    }
}

pub fn mget<K, V, I>(keys: I) -> Cmd
where K: ToRedisArgs,
      V: SingleValue<K>,
      I: Iterator<Item=V>,
{
    let mut cmd = Cmd::new();
    cmd.arg("MGET");
    for k in keys {
        cmd.arg(k.key());
    }
    cmd.clone()
}

/// Commands from https://redis.io/commands/?group=generic
pub trait GenericValue<K: ToRedisArgs>: Key<K> {
    /// Get the expiration time of a key.
    fn ttl(self) -> Cmd
        where Self: Sized
    {
        Cmd::ttl(self.key())
    }

    /// Get the expiration time of a key in milliseconds.
    fn pttl(self) -> Cmd
        where Self: Sized
    {
        Cmd::pttl(self.key())
    }

    fn expire(self, ttl_secs: usize) -> Cmd 
        where Self: Sized
    {
        Cmd::expire(self.key(), ttl_secs)
    }
}

/// Values that can be `SET`, `GET`, etc
pub trait SingleValue<K: ToRedisArgs>: Key<K> {
    fn get<M: ToRedisArgs>(self) -> Cmd
        where Self: Sized
    {
        Cmd::get(self.key())
    }

    fn set<V: ToRedisArgs>(self, val: V) -> Cmd
        where Self: Sized
    {
        Cmd::set(self.key(), val)
    }
}

pub struct SetKey<K: ToRedisArgs> {
    key: K,
}

impl<K: ToRedisArgs> Key<K> for SetKey<K> {
    fn new(key: K) -> Self {
        SetKey { key }
    }

    fn key(self) -> K {
        self.key
    }
}

impl<K: ToRedisArgs> GenericValue<K> for SetKey<K> {}

impl<K: ToRedisArgs> SetKey<K> {
    pub fn sadd<M: ToRedisArgs>(self, member: M) -> Cmd {
        Cmd::sadd(self.key, member)
    }

    pub fn srem<M: ToRedisArgs>(self, member: M) -> Cmd {
        Cmd::srem(self.key, member)
    }

    pub fn smembers(self) -> Cmd {
        Cmd::smembers(self.key)
    }

    pub fn sunion<I: Iterator<Item=SetKey<K>>>(keys: I) -> Cmd {
        let cmd = &mut Cmd::new();
        cmd.arg("SUNION");
        for k in keys {
            cmd.arg(k.key());
        }
        cmd.clone()
    }

    pub fn sinter<I: Iterator<Item=SetKey<K>>>(keys: I) -> Cmd {
        let cmd = &mut Cmd::new();
        cmd.arg("SINTER");
        for k in keys {
            cmd.arg(k.key());
        }
        cmd.clone()
    }
}

#[derive(Clone, Debug)]
pub struct StringKey<K: ToRedisArgs> {
    key: K
}

impl<K: ToRedisArgs> SingleValue<K> for StringKey<K> {
}

impl<K: ToRedisArgs> GenericValue<K> for StringKey<K> {}

impl<K: ToRedisArgs> Key<K> for StringKey<K> {
    fn new(key: K) -> StringKey<K> {
        StringKey { key }
    }

    fn key(self) -> K {
        self.key
    }
}

impl<K: ToRedisArgs> StringKey<K> {
}

pub struct IntKey<K: ToRedisArgs> {
    key: K
}

impl<K: ToRedisArgs> Key<K> for IntKey<K> {
    fn new(key: K) -> IntKey<K> {
        IntKey { key }
    }

    fn key(self) -> K {
        self.key
    }
}

impl<K: ToRedisArgs> SingleValue<K> for IntKey<K> {
}

impl<K: ToRedisArgs> IntKey<K> {
    pub fn incr<A: ToRedisArgs>(self, amount: A) -> Cmd {
        Cmd::incr(self.key, amount)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy)]
    struct Schema;
    impl Schema {
        pub fn myset1(self) -> SetKey<String> {
            SetKey::new("myset1".to_string())
        }
    }

    #[test]
    fn test_with_redis() -> Result<(), Box<dyn std::error::Error>> {
        let client = redis::Client::open("redis://127.0.0.1/")?;
        let mut con = client.get_connection()?;

        let s = Schema;
        s.myset1().del().query(&mut con)?;
        s.myset1().sadd("first").query(&mut con)?;
        let members: Vec<String> =
            s.myset1().smembers().query(&mut con)?;

        assert_eq!(vec!["first"], members);

        Cmd::sadd(&["test_first", "test_second"], "member")
            .query(&mut con)?;

        Ok(())
    }
}
