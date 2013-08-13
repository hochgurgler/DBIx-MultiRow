package DBIx::MultiRow;

use strict;
use warnings;

use Try::Tiny;
use Module::Load;
use MooseX::Types::Moose qw{ Str Defined };
use MooseX::Types::Structured qw{ Dict Map };

=head1 NAME

DBIx::MultiRow - heterogeneous database operations on multiple rows

=head1 SYNOPSIS

    use DBIx::MultiRow;
    use DBI qw{ SQL_INTEGER SQL_TIMESTAMP };

    my %table_info = (
        # The name of the table.
        quoted_table_name => 'foo',

        # If the name of the table needs to be quoted before being
        # included in SQL, then that quoting must be done here.
        # For example, under PostgreSQL table names with upper case or
        # spaces need to be quoted using double quotes, e.g.
        # quoted_table_name => '"Foo"',
        # quoted_table_name => '"foo bar"',

        # Information about all column types which will be addressed
        # (for retrieval or change) in this table.
        columns => {
            'column1' => {
                # dbi_type will be used in bind_value
                dbi_type => SQL_INTEGER,
                # sql_type will be used if a temporary table is required
                sql_type => 'INTEGER NOT NULL'
            },
            'column2' => {
                dbi_type => SQL_INTEGER,
                sql_type => 'INTEGER NOT NULL'
            },
            ...
        }
    );

    # Update multiple rows (potentially with heterogeneous target values)
    multi_update($dbh,
        # table - Mandatory; schema information for the table to update
        table => \%table_info,

        # key_columns - Optional; ArrayRef[ Str ]; default column names to
        # be used for array-style keys
        key_columns => [ qw{ kcol1 kcol2 } ],

        # value_columns - Optional; ArrayRef[ Str ]; default column names to
        # be used for array-style values
        value_columns => [ qw{ vcol1 vcol2 } ],

        # updates - Mandatory; ArrayRef; An array of updates which we want
        # to apply
        updates => [
            # Each element is a two-element ArrayRef containing
            # a key and a value
            [ $key, $value ],

            # $key and $value can take values like the following examples

            # For single-column keys/values, just use a scalar
            [ 1, 'foo' ],
            [ 2, 'bar' ],

            # For multi-column keys/values, use ArrayRefs for the keys
            # and/or values
            [ [ 'Bob',   'Jones'   ], [ 1125, 110 ] ],
            [ [ 'Jane',  'Smith'   ], [ 1200, 130 ] ],
            [ [ 'Jane',  'Austin'  ], [ 1200, 130 ] ],
            [ [ 'Frank', 'Johnson' ], [ 1125, 110 ] ],

            # If keys or values do not all refer to the same set of columns,
            # then column names can be specified explicitly
            [ [ 'Bob',   'Jones'   ], { salary => 1125 } ],
            [ { first_name => 'Jane' }, { bonus => 130 } ],
        ],

        # where - Optional HashRef which says that updates above should
        #    only be applied to rows where the specified columns match the
        #    corresponding values.
        where => { $column1 => $value1, $column2 => $value2, ... },

        # set - Optional HashRef which says that for all rows which are
        #    updated, these columns must also be updated with these values.
        #    Values can either be scalars, in which case they represent
        #    the value to be set in the column, or references to scalars,
        #    in which case they represent literal SQL to be used as the
        #    target column value (e.g.
        #       last_updated => \'NOW()'
        #    )
        set => { $column1 => $value1, $column2 => $value2, ... },
    );

    # Delete multiple rows
    multi_delete($dbh,
        # table - Mandatory; schema information for the table to update
        table => \%table_info,

        # key_columns - Optional; ArrayRef[ Str ]; default column names to
        # be used for array-style keys
        key_columns => [ qw{ kcol1 kcol2 } ],

        # deletes - Mandatory; ArrayRef; An array of updates which we want
        # to apply
        deletes => [
            $key1, $key2, $key3, ...

            # Each key can be

            # a reference to a hash
            { kf1 => $kf1, kf2 => $kf2 },

            # or, if key_fields has been specified, a reference to an array
            [ $kf1, $kf2 ],

            # or, if exactly one key_field has been specified, a scalar
            $key
        ],

        # where - Optional Hashref which says that rows should only be
        #    deleted where the specified columns match the corresponding
        #    values.
        where => { $column1 => $value1, $column2 => $value2, ... },
    );

=head1 DESCRIPTION

This module exists to provide a way of encapsulating the expression
of multi-row database operations, with a view to being able to do various
optimisations such as code factorisation and performance optimisation.

Traditionally, multi-row updates in DBI would take the approach of

    prepare

    execute
    execute
    execute
    ...

But this can result in a large number of update queries being the dominant
factor in exectution time.  Reducing the number of queries may lead to
considerable improvements in performance.

=head2 Examples

Say you have a table like this

    id    valid

    1     FALSE
    2     FALSE
    3     TRUE

Say your code had determined that records 1 and 2 were now valid.  A
simplistic way to make those updates would be

    UPDATE table SET valid = TRUE WHERE id = 1;
    UPDATE table SET valid = TRUE WHERE id = 2;

As the set of column values set by the SET clauses

    valid = TRUE

are identical, we can collapse the two update queries into a single query

    UPDATE table SET valid = TRUE WHERE id IN (1, 2);

This optimisation is implemented in the core engine.

Say you have a table like this

    name   salary

    Bob    1000
    Jane   1100
    Frank  1050

Say we have new salaries for all three people.  Bob's salary is changing
to 1125, Jane's to 1200, and Frank's also 1125.  We could update our database
using three update queries:

    UPDATE table SET salary = 1125 WHERE name = 'Bob';
    UPDATE table SET salary = 1200 WHERE name = 'Jane';
    UPDATE table SET salary = 1125 WHERE name = 'Frank';

We can collapse the updates for Bob and Frank into a single query.

    UPDATE table SET salary = 1125 WHERE name IN ('Bob', 'Frank');
    UPDATE table SET salary = 1200 WHERE name = 'Jane';

Collapsing multiple update queries into fewer update queries is the
role of DBIx::MultiRow. In this example we would write:

    multi_update($dbh,
        \%table_info,
        key_columns   => [qw{ name }],
        value_columns => [qw{ salary }],
        updates => [
            [ 'Bob',   1125 ],
            [ 'Jane',  1200 ],
            [ 'Frank', 1125 ],
        ],
    );

In the general case, the key which identifies rows to be updated could have
more than one column, and more than one column may be being updated.

Say we have a table like this

    first_name last_name salary bonus

    Bob        Jones     1000   100
    Jane       Smith     1100   105
    Jane       Austin    1100   105
    Frank      Johnson   1050   90

And we want to update salaries and bonuses for multiple people.  We could
write:

    multi_update($dbh,
        \%table_info,
        key_columns   => [qw{ first_name last_name }],
        value_columns => [qw{ salary bonus }],
        updates => [
            [ [ 'Bob',   'Jones'   ], [ 1125, 110 ] ],
            [ [ 'Jane',  'Smith'   ], [ 1200, 130 ] ],
            [ [ 'Jane',  'Austin'  ], [ 1200, 130 ] ],
            [ [ 'Frank', 'Johnson' ], [ 1125, 110 ] ],
        ],
    );

In some cases we will have conditions which apply to the whole update,
and common changes which need to be made to all rows.

    company  first_name last_name salary bonus last_updated

    WidgetCo Bob        Jones     1000   100   20130101T12:30
    WidgetCo Jane       Smith     1100   105   20130101T12:30
    WidgetCo Jane       Austin    1100   105   20130101T12:30
    WidgetCo Frank      Johnson   1050   90    20130101T12:30
    Frobnitz Susan      Black     1000   100   20130101T12:30
    Frobnitz Bob        Jones     1000   100   20130101T12:30

We want to update salary and bonus for people only in the company
'WidgetCo'.  "Bob Jones" appears in both companies, so we need to be
careful to identify the company, but we don't want to have to specify it
repeatedly for each row.  The 'where' parameter can be used to specify
a global condition.

Another thing which we may want to do is update a column with the same
value for all rows being updated.  Rather than have to specify
the same value over and over again, we can use the 'set' parameter.

The 'set' parameter takes a HashRef whose keys are the columns to be
updated.  Values specified as scalars are applied as-is, while values
specified as ScalarRef specify literal SQL.

    multi_update($dbh,
        \%table_info,
        key_columns   => [qw{ first_name last_name }],
        value_columns => [qw{ salary bonus }],
        updates => [
            [ [ 'Bob',   'Jones'   ], [ 1125, 110 ] ],
            [ [ 'Jane',  'Smith'   ], [ 1200, 130 ] ],
            [ [ 'Jane',  'Austin'  ], [ 1200, 130 ] ],
            [ [ 'Frank', 'Johnson' ], [ 1125, 110 ] ],
        ],
        where => { company => 'WidgetCo' },
        set => { last_update => \'NOW()' }
    );

Similarly, to delete this set of rows:

    multi_delete($dbh,
        \%table_info,
        key_columns => [qw{ first_name last_name }],
        deletes     => [
            [ 'Bob',   'Jones'   ],
            [ 'Jane',  'Smith'   ],
            [ 'Jane',  'Austin'  ],
            [ 'Frank', 'Johnson' ],
        ],
        where       => { company => 'WidgetCo' },
    );


=head1 NOTES

None of the functions in this module do their own database transaction,
but in many cases issue multiple queries.  Callers should consider arranging
for calls to be inside a database transaction.

multi_update and multi_delete make no guarantees about the order in which
rows are updated or deleted.  Under some databases (e.g. PostgreSQL),
row locks will be acquired when rows are updated or deleted.
Callers should consider taking steps to avoid deadlocking, such as
wrapping calls to this package in a transaction and using

    SELECT ... ORDER BY ... FOR UPDATE

to acquire row locks in a consistent order.

=cut

use MooseX::Types -declare => [qw{ DBIxMultiRowTable }];

use Sub::Exporter -setup => {
    exports => [ qw{ multi_update multi_delete DBIxMultiRowTable } ]
};

subtype DBIxMultiRowTable,
    as Dict[
        quoted_table_name => Str,
        columns => Map[
            Str, Dict[
                dbi_type => Defined,
                sql_type => Str
            ]
        ]
    ];

sub multi_update { return _forward_to_engine(@_) }

sub multi_delete { return _forward_to_engine(@_) }

sub _forward_to_engine {
    my $dbh = shift;
    my $calling_function_name = (caller 1)[3] =~ s{^.*::}{}sr;

    # At this point we want to detect the type of the connected database so
    # that we can try to instantiate an object of a database-specific
    # engine subclass.  However, there doesn't appear to be an easy way to do
    # this detection.  We can have a look at $dbh->{Driver}->{Name}, and this
    # should give us an idea of the DBD:: driver in use, though that doesn't
    # necessarily tell us what the type of the connected database is.
    # However, at the time of writing, our only database-specific engine
    # subclass is for PostgreSQL (DBIx::MultiRow::Engine::DB::Pg), which can
    # be detected by this means.
    # If it is desired to write a database-specific engine subclass which
    # can't be detected by this means, then this detection will need to be
    # refined.
    # (At the time of writing, there is some code in DateTime::Format::DBI
    # which appears to do something a bit cleverer, so we may want to see if
    # we can do something with that.)
    my $db_driver_name = $dbh->{Driver}->{Name};

    my $base_engine_class = __PACKAGE__ . '::Engine';

    my $db_type_specific_class = "${base_engine_class}::DB::${db_driver_name}";

    my $engine;

    try {
        Module::Load::load($db_type_specific_class);
        $engine = $db_type_specific_class->new({ dbh => $dbh });
    } catch {
        Module::Load::load($base_engine_class);
        $engine = $base_engine_class->new({ dbh => $dbh });
    };

    return $engine->$calling_function_name(@_);
}

1;

__END__

=head1 AUTHOR

William Blunn <william.blunn@net-a-porter.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2013 by Net-a-porter.com.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut
