# DBIish::Transaction

```raku
use DBIish::Transaction;
use DBIish::Savepoint;

my $t = DBIish::Transaction.new(connection => {DBIish.connect('Pg', :$database);}, :retry);

$t.in-transaction: -> $dbh {
    # BEGIN issued at start
    
    $dbh.do(q{CREATE TABLE tab (col integer)});
    
    my $sth = $dbh.prepare('INSERT INTO tab VALUES ($1);');
    $sth.execute(1);
    
    # Also allows for savepoints on databases supporting this behaviour.
    # These are kinda like sub-transactions. Catch the exception to prevent the
    # outer transaction from being rolled back.
    try {
        my $sp = DBIish::Savepoint.new(connection => $dbh);
        $sp.in-savepoint: -> $sp-dbh {
            # SAVEPOINT issued at start
            
            my $updsth = $sp-dbh.prepare('UPDATE tab SET col = col + 1');
            $updsth.execute();
            
            $sth.execute('Insert Invalid Value');
            
            # ROLLBACK TO <savepoint> issued due to the above failure.
        }
    }
    
    # COMMIT issued at end
    # Table "tab" contains a single record with col = 1
}

$t.in-transaction: -> $dbh {
    my $sth = $dbh.prepare('INSERT INTO t VALUES ($1);');
    $sth.execute(2);
    
    fail('Changed my mind about the insert');
    
    # ROLLBACK due to the error
}
```

## Description 

This is a easy to use way of creating database transactions that always commit/rollback and can retry on temporary
failures such as disconnect, deadlocks, serialization issues, or snapshot age.

## DBIish::Transaction

```raku
DBIish::Transaction.new(:connection, :retry, :max-retry-count, :begin, :rollback, :after-rollback, :commit);
```

### :connection

Either a DBDish::Connection, or a Callable which returns a DBDish::Connection. If a Callable is provided transactions
may be retried if disconnect occurs when :retry is specified. A connection provided by a callable will be disposed of on
completion of the transaction.

 
### :begin($dbh)

```raku
{  $_.do(q{BEGIN})  } 
```

By default this is a Callable which performs the simplest begin statement. You may want to modify transaction behaviour.
Serializable Isolation level is highly recommended on supported products as this mode eliminates many potential
errors due to otherwise silent race conditions.

In the below example using `SERIALIZABLE` and `:retry`, the transaction will be attempted up to 4 times during
serialization errors. This allows safe Raku read/modify/write without needing to worry about locking for race
conditions or unexpected failure.

```raku
my $id = 1;

my $t = DBIish::Transaction.new(:retry,
    connection => { DBIish.connect('Pg', :$database) },
    begin => -> $dbh { $dbh.do(q{ BEGIN ISOLATION LEVEL SERIALIZABLE } ) },
    :retry
).in-transaction: -> $dbh {
    my $sth = $dbh.prepare('SELECT col FROM tabl WHERE id = $1');
    $sth.execute($id);
    my $row = $sth.row(:hash);

    my $sth = $dbh.prepare('UPDATE INTO tabl SET col = $2 WHERE id = $1');
    $sth.execute( $id, $row<col> * complex_function() );
}
```

### :rollback($dbh)

```raku
{  $_.do(q{ROLLBACK})  } 
```

By default this is a Callable which performs the simplest rollback statement.

NOTE: `AND CHAIN` type modifications will require you to provide some non-trivial logic in the `begin` callable.

### :after-rollback(Int $transaction-retry-attempt)

Callable which will be called after a rollback is attempted. This is useful for resetting state for another attempt at
the DB transaction work, or for logging/debugging purposes.

### :commit($dbh)

```raku
{  $_.do(q{COMMIT})  } 
```

By default this is a Callable which performs the simplest commit statement.  

NOTE: `AND CHAIN` type modifications will require you to provide some non-trivial logic in the `begin` callable.

### :retry

Catches errors [marked temporary](https://github.com/raku-community-modules/DBIish#statement-exceptions) by DBIish. The
transaction in progress will be rolled back and a new transaction started. The function body is expected to be
idempotent for non-database work as it may be executed multiple times.

If `:connection` is provided a function, this will retry on a database connectivity issue as well by establishing
a new connection and attempting to execute the transaction body.

### :max-retry-count

Number of times to retry the work after a temporary failure before giving up.

3 by default.


## DBIish::Savepoint

```raku
DBIish::Transaction.new(:connection, :begin, :rollback, :commit);
```

### :connection

A DBDish::Connection with a currently active transaction.

### :begin($dbh)

```raku
{  $_.do(q{SAVEPOINT <random name>})  } 
```

By default this is a Callable which performs the simplest `SAVEPOINT` statement. A random name is selected.

### :rollback($dbh)

```raku
{  $_.do(q{ROLLBACK TO SAVEPOINT <random name>})  } 
```

By default this is a Callable which performs the simplest `ROLLBACK TO SAVEPOINT` statement.

### :after-rollback()

Callable which will be called after a rollback is attempted. This is useful for resetting state for another attempt,
or for logging/debugging purposes.

### :release($dbh)

```raku
{  $_.do(q{RELEASE SAVEPOINT})  } 
```

By default this is a Callable which performs the simplest `RELEASE SAVEPOINT` statement. This free's the savepoint
related resources on the database side.


## LICENSE

All files in this repository are licensed under the terms of the Creative Commons CC0 License; for details,
please see the LICENSE file