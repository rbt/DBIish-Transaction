#!/usr/bin/env perl6

use v6;
use DBIish;
use DBIish::Transaction;
use DBIish::Savepoint;
use Test;

plan 20;

# TODO: Should match all pg functionality.
#   - Simulate serialization failures
#   - Force network failure (disconnect) during transaction

my %con-parms = :database<dbdishtest>, :user<testuser>, :password<testpass>;
my $dbh;

try {
  $dbh = DBIish.connect('mysql', |%con-parms);
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
$dbh.do(q{CREATE TABLE dbdish_t_test ( item varchar(255) NOT NULL )});

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

            my $sth = $dbh.prepare(q{INSERT INTO dbdish_t_test VALUES (?)});
            $sth.execute($attempt-count.Str);

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

            my $sth = $dbh.prepare(q{INSERT INTO dbdish_t_test VALUES (?)});
            $sth.execute($attempt-count.Str);

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

            my $sth = $dbh.prepare(q{INSERT INTO dbdish_t_test VALUES (?)});
            $sth.execute($attempt-count.Str);

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

            my $sth = $dbh.prepare(q{INSERT INTO dbdish_t_test VALUES (?)});
            $sth.execute($attempt-count.Str);

            'A Value';
        }
    }, 'Return with a value okay';

    my $sth = $dbh.prepare('SELECT item FROM dbdish_t_test');
    $sth.execute();
    my @rows = $sth.allrows(:array-of-hash);
    is @rows.elems, 1, 'Committed inserted value';
    is $ret, 'A Value', 'Return value maintained';
}


# Rollback savepoint #1, keep savepoint #2, commit transaction.
{
    $dbh.do('TRUNCATE dbdish_t_test');

    lives-ok {
        DBIish::Transaction.new(connection => $dbh, :retry).in-transaction: {
            my $dbh = $_;

            my $sth = $dbh.prepare(q{INSERT INTO dbdish_t_test VALUES (?)});
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

            my $sth = $dbh.prepare(q{INSERT INTO dbdish_t_test VALUES (?)});
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