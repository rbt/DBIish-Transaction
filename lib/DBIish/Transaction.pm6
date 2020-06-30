use v6;

use DBIish;
use DBDish::Connection;

unit class DBIish::Transaction;

has $.connection is required;

has Int $!retry-count = 0;
has Int $.max-retry-count = 3;
has Bool $.retry;

# Useful for debugging and logging purposes.
has @.exception-stack;

has Callable $.begin = {
    $_.do(q{BEGIN});
};

has Callable $.rollback = {
    $_.do(q{ROLLBACK});
};

has Callable $.after-rollback;

has Callable $.commit = {
    $_.do(q{COMMIT});
};

method in-transaction(Callable $code) {
    my DBDish::Connection $dbh;

    my $ret;
    my $want-dispose = False;
    my $finished = False;
    repeat until $finished {
        # Try to retrieve a valid connection if the last used connection was not valid.
        unless $dbh and $dbh.ping {
            given $.connection {
                when Callable {
                    $dbh = ($.connection)();
                    $want-dispose = True;
                }
                when DBDish::Connection {
                    $dbh = $.connection;
                }
                default {
                    die(q{Unknown connection type "%s".}.sprintf($.connection.^name));
                }
            }
        }

        ($.begin)($dbh);
        $ret = $code($dbh);
        ($.commit)($dbh);
        $finished = True;

        # If retry is enabled, catch and clear temporary DB related errors such as serialization failures, deadlocks,
        # old snapshots, and network disconnects; up to max-retry-count times.
        #
        # FIXME: Fail() is processed by the CATCH block. Can it be repassed upstream without turning into an exception?
        CATCH {
            my $ex = $_;

            # Useful for debugging;
            @.exception-stack.push($ex);

            # Rollback if the connection is valid
            ($.rollback)($dbh) if $dbh.ping;
            ($.after-rollback)($!retry-count) with $.after-rollback;

            when ($.retry and $!retry-count < $.max-retry-count) {
                when X::DBDish::DBError {
                    when so ($ex.can('is-temporary') and $ex.is-temporary) {
                        $!retry-count += 1;
                    }
                    $ex.rethrow;
                }
                when X::DBDish {
                    when $ex ~~ /"no connection to the server"/ {
                        $!retry-count += 1;
                    }
                    $ex.rethrow;
                }
                default {
                    $ex.rethrow;
                }
            }
            default {
                $ex.rethrow;
            }
        }

        LEAVE {
            if $want-dispose {
                $dbh.dispose;
                $dbh = Nil;
            }
        }
    }

    return $ret;
}
