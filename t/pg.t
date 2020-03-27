#!/usr/bin/env perl6

use v6;
use DBIish;
use DBIish::Transaction;
use DBIish::Savepoint;
use Test;

plan 37;

my %con-parms;
# If env var set, no parameter needed.
%con-parms<database> = 'dbdishtest' unless %*ENV<PGDATABASE>;
%con-parms<user> = 'postgres' unless %*ENV<PGUSER>;
my $dbh;

try {
  $dbh = DBIish.connect('Pg', |%con-parms);
  CATCH {
        when X::DBIish::LibraryMissing | X::DBDish::ConnectionFailed {
        diag "$_\nCan't continue.";
        }
            default { .throw; }
  }
}
without $dbh {
    skip-rest 'prerequisites failed';
    exit;
}

# Real table required for disconnect tests
$dbh.do(q{DROP TABLE IF EXISTS dbdish_t_test});
$dbh.do(q{CREATE TABLE dbdish_t_test ( item text NOT NULL )});

# Hard failure passed upstream on first try
{
    $dbh.do('TRUNCATE dbdish_t_test');

    my $attempt-count = 0;
    throws-like {
        my $t = DBIish::Transaction.new(connection => $dbh, :retry).in-transaction: {
            my $dbh = $_;
            $attempt-count += 1;

            my $sth = $dbh.prepare(q{INSERT INTO dbdish_t_test VALUES (NULL)});
            $sth.execute();
        }
    }, X::DBDish::DBError, 'Insert failed';

    is $attempt-count, 1, 'Single attempt made';
}

# Serialization failure, then failure
{
    $dbh.do('TRUNCATE dbdish_t_test');

    my $attempt-count = 0;
    throws-like {
        DBIish::Transaction.new(connection => $dbh, :retry).in-transaction: {
            my $dbh = $_;
            $attempt-count += 1;

            my $sth = $dbh.prepare(q{INSERT INTO dbdish_t_test VALUES ($1)});
            $sth.execute($attempt-count);

            # Mock a serialization failure.
            if $attempt-count <= 2 {
                $dbh.do(q:to/SQL/);
                DO LANGUAGE plpgsql $$BEGIN RAISE EXCEPTION 'Pretend serialization failure' USING ERRCODE = '40001'; END;$$;
                SQL
        } else {
                $sth.execute(Nil);
            }
        }
    }, X::DBDish::DBError, 'Rollback after serialization retests';

    my $sth = $dbh.prepare('SELECT item from dbdish_t_test');
    $sth.execute();
    my @rows = $sth.allrows(:hash);
    is @rows.elems, 0, 'Empty set';
    is $attempt-count, 3, '3 attempts';
}

# Serialization failure, then success
{
    $dbh.do('TRUNCATE dbdish_t_test');

    my $attempt-count = 0;
    lives-ok {
        DBIish::Transaction.new(connection => $dbh, :retry).in-transaction: {
            my $dbh = $_;
            $attempt-count += 1;

            my $sth = $dbh.prepare(q{INSERT INTO dbdish_t_test VALUES ($1)});
            $sth.execute($attempt-count);

            # Mock a serialization failure.
            unless $attempt-count > 2 {
                $dbh.do(q:to/SQL/);
                DO LANGUAGE plpgsql $$BEGIN RAISE EXCEPTION 'Pretend serialization failure' USING ERRCODE = '40001'; END;$$;
                SQL
            }
        }
    }, 'Survives serialization failures';

    my $sth = $dbh.prepare('SELECT item FROM dbdish_t_test');
    $sth.execute();
    my @rows = $sth.allrows(:array-of-hash);
    is @rows.elems, 1, 'Single record';
    is $attempt-count, 3, '3 Attempts made';
    is @rows[0]<item>, 3, 'Attempt 3 was committed';
}

# Disconnect failures, then success
{
    $dbh.do('TRUNCATE dbdish_t_test');

    my $attempt-count = 0;
    lives-ok {
        DBIish::Transaction.new(connection => {DBIish.connect('Pg', |%con-parms);}, :retry).in-transaction: {
            my $dbh2 = $_;
            $attempt-count += 1;

            my $sth = $dbh2.prepare(q{INSERT INTO dbdish_t_test VALUES ($1)});
            $sth.execute($attempt-count);

            # Disconnect for first couple of loops.
            unless $attempt-count > 2 {
                my $sthterminate = $dbh2.prepare(q{SELECT pg_terminate_backend(pg_backend_pid())});
                $sthterminate.execute();
            }
        }
    }, 'Survives network disruption';

    my $sth = $dbh.prepare('SELECT item FROM dbdish_t_test');
    $sth.execute();
    my @rows = $sth.allrows(:array-of-hash);
    is @rows.elems, 1, 'Single record';
    is $attempt-count, 3, '3 Attempts made';
    is @rows[0]<item>, 3, 'Attempt 3 was committed';
}

# Return Fail
# Should this live and return the Failure object instead?
{
    $dbh.do('TRUNCATE dbdish_t_test');

    my $attempt-count = 0;
    my $ret;
    throws-like {
        my $t = DBIish::Transaction.new(connection => $dbh, :retry);
        $ret = $t.in-transaction: {
            my $dbh = $_;
            $attempt-count += 1;

            my $sth = $dbh.prepare(q{INSERT INTO dbdish_t_test VALUES ($1)});
            $sth.execute($attempt-count);

            fail('User decided to rollback');
        }
    }, X::AdHoc, 'User fail processed as error';

    my $sth = $dbh.prepare('SELECT item FROM dbdish_t_test');
    $sth.execute();
    my @rows = $sth.allrows(:array-of-hash);
    is @rows.elems, 0, 'Rolled back insert on failure';
    is $ret, Any, 'Empty return maintained';
}

# Thrown exception, not from DBIish
{
    $dbh.do('TRUNCATE dbdish_t_test');

    my $attempt-count = 0;
    my $ret;
    my $t = DBIish::Transaction.new(connection => $dbh, :retry);
    throws-like {
        $ret = $t.in-transaction: {
            my $dbh = $_;
            $attempt-count += 1;

            my $sth = $dbh.prepare(q{INSERT INTO dbdish_t_test VALUES ($1)});
            $sth.execute($attempt-count);

            die('Some Exception');
        }
    }, 'Some Exception', 'Exception is rethrown';

    my $sth = $dbh.prepare('SELECT item FROM dbdish_t_test');
    $sth.execute();
    my @rows = $sth.allrows(:array-of-hash);
    is @rows.elems, 0, 'Rolled back insert on exception';
}

# Return empty
{
    $dbh.do('TRUNCATE dbdish_t_test');

    my $attempt-count = 0;
    my $ret;
    lives-ok {
        my $t = DBIish::Transaction.new(connection => $dbh, :retry);
        $ret = $t.in-transaction: {
            my $dbh = $_;
            $attempt-count += 1;

            my $sth = $dbh.prepare(q{INSERT INTO dbdish_t_test VALUES ($1)});
            $sth.execute($attempt-count);

            Nil;
        }
    }, 'Empty return okay';

    my $sth = $dbh.prepare('SELECT item FROM dbdish_t_test');
    $sth.execute();
    my @rows = $sth.allrows(:array-of-hash);
    is @rows.elems, 1, 'Committed inserted value';
    is $ret, Any, 'Any/Nil Return value maintained';
}

# Return a value
{
    $dbh.do('TRUNCATE dbdish_t_test');

    my $attempt-count = 0;
    my $ret;
    lives-ok {
        my $t = DBIish::Transaction.new(connection => $dbh, :retry);
        $ret = $t.in-transaction: {
            my $dbh = $_;
            $attempt-count += 1;

            my $sth = $dbh.prepare(q{INSERT INTO dbdish_t_test VALUES ($1)});
            $sth.execute($attempt-count);

            'A Value';
        }
    }, 'Return with a value okay';

    my $sth = $dbh.prepare('SELECT item FROM dbdish_t_test');
    $sth.execute();
    my @rows = $sth.allrows(:array-of-hash);
    is @rows.elems, 1, 'Committed inserted value';
    is $ret, 'A Value', 'Return value maintained';
}

# DB errors within the savepoint reset the transaction state.
{
    $dbh.do('TRUNCATE dbdish_t_test');

    my $transaction-attempt-count = 0;
    my $savepoint-attempt-count = 0;
    lives-ok {
        DBIish::Transaction.new(connection => $dbh, :retry).in-transaction: {
            my $dbh = $_;

            $transaction-attempt-count += 1;
            my $sth = $dbh.prepare(q{INSERT INTO dbdish_t_test VALUES ($1)});
            $sth.execute('Top insert %d'.sprintf($transaction-attempt-count));


            DBIish::Savepoint.new(connection => $dbh).in-savepoint: {
                my $dbh = $_;

                $savepoint-attempt-count += 1;
                $sth.execute('Subtrans attempt %d'.sprintf($savepoint-attempt-count));

                # Mock a serialization failure. This resets the transaction, not the savepoint.
                unless $transaction-attempt-count > 2 {
                    $dbh.do(q:to/SQL/);
                    DO LANGUAGE plpgsql $$BEGIN RAISE EXCEPTION 'Pretend serialization failure' USING ERRCODE = '40001'; END;$$;
                    SQL
                }
            }
        }
    }, 'Survive temporary failures in savepoint';

    my $sth = $dbh.prepare('SELECT item FROM dbdish_t_test');
    $sth.execute();
    my @rows = $sth.allrows(:array-of-hash);
    is @rows.elems, 2, 'Two records';
    is $transaction-attempt-count, 3, '3 Attempt at transaction level';
    is $savepoint-attempt-count, 3, '3 Attempts at savepoint level';
    is @rows[0]<item>, 'Top insert 3', 'Outer transaction repeated 3 tries';
    is @rows[1]<item>, 'Subtrans attempt 3', 'Savepoint repeated 3 tries';
}

# Rollback savepoint #1, keep savepoint #2, commit transaction.
{
    $dbh.do('TRUNCATE dbdish_t_test');

    lives-ok {
        DBIish::Transaction.new(connection => $dbh, :retry).in-transaction: {
            my $dbh = $_;

            my $sth = $dbh.prepare(q{INSERT INTO dbdish_t_test VALUES ($1)});
            $sth.execute('Transaction Start');

            try {
                DBIish::Savepoint.new(connection => $dbh).in-savepoint: {
                    my $dbh = $_;

                    $sth.execute('Savepoint #1');
                    fail('Eat error without corrupting db transaction');
                }
            }

            try {
                DBIish::Savepoint.new(connection => $dbh).in-savepoint: {
                    my $dbh = $_;

                    $sth.execute('Savepoint #2');
                }
            }

            $sth.execute('Transaction End');
        }
    }, 'Survive Failure in savepoint';

    my $sth = $dbh.prepare('SELECT item FROM dbdish_t_test');
    $sth.execute();
    my @rows = $sth.allrows(:array-of-hash);
    is @rows.elems, 3, '3 records';
    is @rows[0]<item>, 'Transaction Start', 'Outer transaction kept';
    is @rows[1]<item>, 'Savepoint #2', 'Finished savepoint kept';
    is @rows[2]<item>, 'Transaction End', 'Outer transaction finishes';
}

# Savepoint exception cascades up to transaction.
{
    $dbh.do('TRUNCATE dbdish_t_test');

    throws-like {
        DBIish::Transaction.new(connection => $dbh, :retry).in-transaction: {
            my $dbh = $_;

            my $sth = $dbh.prepare(q{INSERT INTO dbdish_t_test VALUES ($1)});
            $sth.execute('Transaction Start');

            DBIish::Savepoint.new(connection => $dbh).in-savepoint: {
                my $dbh = $_;

                $sth.execute('Savepoint #1');
                die('Transaction aborts without try/catch around savepoint');
            }
        }
    }, X::AdHoc, 'Exception in savepoint cascades up';

    my $sth = $dbh.prepare('SELECT item FROM dbdish_t_test');
    $sth.execute();
    my @rows = $sth.allrows(:array-of-hash);
    is @rows.elems, 0, '0 records';
}

# Cleanup
$dbh.do(q{DROP TABLE IF EXISTS dbdish_t_test});
$dbh.dispose;