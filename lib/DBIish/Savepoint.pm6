use v6;

use DBDish::Connection;

unit class DBIish::Savepoint;

has DBDish::Connection $.connection is required;

has $!random-name = 'dbiish_' ~ ('a' .. 'z').flat.pick(5).join(q{});

has Callable $.begin = {
    $_.do(q{SAVEPOINT %s}.sprintf($_.quote($!random-name, :as-id)));
};

has Callable $.rollback = {
    $_.do(q{ROLLBACK TO SAVEPOINT %s}.sprintf($_.quote($!random-name, :as-id)));
};

has Callable $.after-rollback;

has Callable $.release = {
    $_.do(q{RELEASE SAVEPOINT %s}.sprintf($_.quote($!random-name, :as-id)));
};

# Savepoints are not retried. Typical issues like deadlocks, connection loss, serialization failures, and
# old snapshots all require a full transaction restart.
method in-savepoint(Callable $code) {
    my $dbh = $.connection;

    ($.begin)($dbh);
    my $ret = $code($dbh);

    if $ret ~~ Failure {
        ($.rollback)($dbh);
    } else {
        ($.release)($dbh);
    }

    CATCH {
        my $ex = $_;

        # Rollback if the connection is valid
        ($.rollback)($dbh) if $dbh.ping;
        ($.after-rollback)() with $.after-rollback;

        default {
            $ex.rethrow;
        }
    }

    return $ret;
}
