package DBIx::MultiRow::Engine;

use strict;
use warnings;

=head1 NOTE

This is the internal gubbins private to DBIx::MultiRow.  Do not call it directly.

Only go in here if you are working on it or an extension or something like that.

=cut

use Carp;
use Data::Alias;
use Moose;
use Moose::Util::TypeConstraints;
use MooseX::Params::Validate;
use MooseX::Types::Moose qw{ ArrayRef HashRef Str Defined Item CodeRef ScalarRef Undef Any };
use MooseX::Types::Structured qw{ Dict Map Tuple };
use Data::Dump 'pp';

use DBIx::MultiRow 'DBIxMultiRowTable';

has dbh => ( is => 'rw' );

has table => ( is => 'rw', isa => DBIxMultiRowTable );
has global_where => ( is => 'rw', isa => HashRef );
has global_set => ( is => 'rw', isa => HashRef );

=head2 multi_update

    table => <DBIxMultiRowTable>,
    key_columns => [qw{ kc1 kc2 }],
    value_columns => [qw{ vc1 vc2 }],
    updates => [
      # arrayref keys are interpreted using @$key_columns
      [ [ 1, 2 ], [qw{ foo bar }] ],
      [ [ 3, 4 ], [qw{ baz qux }] ],

      # key hashrefs specify their own columns, and ignore @$key_columns
      [ { kc3 => 5 }, [qw{ foo bar }] ],

      # value hashrefs specify their own columns, and ignore @$value_columns
      [ [ 6, 7 ], { vc3 => 'baz', vc4 => 'qux', vc5 => 'corge' } ],

      # Supports both the key and value being hashrefs (ignoring both
      # @$key_columns and @$value_columns)
      [ { kc3 => 8 }, { vc3 => 'baz', vc4 => 'qux', vc5 => 'corge' } ],
    ],
    where => { company => 'WidgetCo' },
    set => { last_updated => \'NOW()' }

=head3 Approach

Three layers of bucketisation: Key-Column, Value-Column, Value to group
the updates into fewer SQL UPDATE statements.

=head3 KEY-COLUMN bucketisation

This implementation bucketises the updates according to the set of
key columns affected.  So, for example, all updates which affect
kc1 & kc2 will go into one bucket, and all updates which affect
only kc3 will go into another bucket.

=cut

sub multi_update {
    my ($self,
        $table, $key_columns, $value_columns, $updates, $where, $set
    ) = validated_list(
        \@_,
        table         => { isa => DBIxMultiRowTable },
        key_columns   => { isa => ArrayRef[ Str ], optional => 1 },
        value_columns => { isa => ArrayRef[ Str ], optional => 1 },
        updates       => { isa => ArrayRef[ Tuple[ Any, Any ] ] },
        where         => { isa => HashRef, optional => 1 },
        set           => { isa => HashRef, optional => 1 }
    );

    $self->table($table);
    $self->global_where($where) if defined $where;
    $self->global_set($set) if defined $set;

    # Bucketise by set of key columns

    # To use a hash for bucketising, we need to flatten the set of key columns
    # in each update down to a string.

    # The updates, but bucketised by the set of key columns.
    # So all updates which use the same set of columns to key off will be in
    # the same bucket.
    my %updates_by_key_column_set;

    foreach my $update (@$updates) {
        my ($key, $value) = @$update;

        my ($canon_key) = $self->canonicalise_key_or_value(
            (defined $key_columns ? (columns => $key_columns) : ()),
            key_or_value => $key
        );

        my @update_key_columns_sorted = sort keys %$canon_key;

        alias my @update_key = @$canon_key{ @update_key_columns_sorted };

        my $bucket_id = $self->serialise(\@update_key_columns_sorted);

        push @{ $updates_by_key_column_set{$bucket_id} },
            [ \@update_key, $value ];
    }

    my $update_count = 0;

    # Process each bucket separately
    foreach my $bucket_id (sort keys %updates_by_key_column_set) {
        my $key_columns = $self->deserialise($bucket_id);

        $update_count += $self->multi_update_for_1_key_col_set(
            key_columns => $key_columns,
            (defined $value_columns ? (value_columns => $value_columns) : ()),
            updates => $updates_by_key_column_set{$bucket_id}
        );
    }

    return $update_count;
}

=head2 canonicalise_key_or_value

Given an array of (default) column names, and a single `key' (WHERE thing) or
`value' (SET thing) in any of the three possible forms
(scalar, ArrayRef, HashRef), return the `key_or_value' in canonical form (a HashRef)

Given

    columns      => [qw{ c1 }],
    key_or_value => 'foo'

Returns

    { c1 => 'foo' }

Given

    columns      => [qw{ c1 c2 }],
    key_or_value => [qw{ foo bar }]

Returns

    { c1 => 'foo', c2 => 'bar' }

Given

    columns      => [qw{ quack bark oink }],
    key_or_value => { c1 => 'foo', c2 => 'bar' }

Returns

    { c1 => 'foo', c2 => 'bar' }

ignoring the columns.

=cut

sub canonicalise_key_or_value {
    my ($self, %p) = @_;

    my $key_or_value = $p{key_or_value};
    my $ref_key_or_value = ref $key_or_value;

    if ($ref_key_or_value eq 'HASH') {
        return $key_or_value;
    }

    if (($ref_key_or_value eq 'ARRAY') || !$ref_key_or_value) {
        $key_or_value = [ $key_or_value ] if !$ref_key_or_value;

        if (defined $p{columns}) {
            if (scalar @$key_or_value != scalar @{$p{columns}}) {
                confess 'Mismatch between key_or_value component count and key_or_value columns count';
            }

            my %canon_key_or_value;
            alias @canon_key_or_value{ @{$p{columns}} } = @$key_or_value;

            return \%canon_key_or_value;
        } else {
            croak 'Array key_or_value used but key_or_value columns not specified';
        }
    } else {
        croak 'Invalid format for key_or_value';
    }
}

=head2 multi_update_for_1_key_col_set

    key_columns   => [qw{ kc1 kc2 }], # things you're looking for (WHERE)
    value_columns => [qw{ vc1 vc2 }], # default columns you want to SET

    # The updates as a list of key/value pairs (not bucketised)
    #
    # All `keys' (first element) *must* use `key_columns' and must be arrays.
    #
    # `values' (second element) can be either arrayrefs (matching
    # `value_columns') or hashrefs (which can have different sets of columns
    # to be set, specified by the hash keys)
    #
    updates       => [
        [ [ 1,  2 ], [qw{ foo bar }] ],
        [ [ 3,  4 ], [qw{ foo bar }] ],
        [ [ 5,  6 ], [qw{ baz qux }] ],
        [ [ 7,  8 ], [qw{ baz qux }] ],
        [ [ 9, 10 ], { vc1 => 'baz', vc2 => 'qux' },
        [
            [ 11, 12 ], # key
            { vc1 => 'corge' } # setting only vc1 (not vc1 & vc2)
        ],
    ],

=head3 VALUE-COLUMN bucketisation

This implementation bucketises the updates according to the set of
value columns affected.  So, for example, all updates which affect
vc1 & vc2 will go into one bucket, and all updates which affect
only vc1 will go into another bucket.

The buckets are then passed, one at a time, to multi_update_for_1_key_col_set
for further processing.

=cut

sub multi_update_for_1_key_col_set {
    my ($self,
        $key_columns, $value_columns, $updates
    ) = validated_list(
        \@_,
        key_columns   => { isa => ArrayRef[ Str ] },
        value_columns => { isa => ArrayRef[ Str ], optional => 1 },
        updates       => { isa => ArrayRef }
    );

    # Bucketise by set of value columns

    # To use a hash for bucketising, we need to flatten the set of value
    # columns in each update down to a string.

    # The updates, but bucketised by the set of value columns.
    # So all updates which are setting the same set columns to key off will be
    # in the same bucket.
    my %updates_by_value_column_set;

    foreach my $update (@$updates) {
        my ($key, $value) = @$update;

        my ($canon_value) = $self->canonicalise_key_or_value(
            (defined $value_columns ? (columns => $value_columns) : ()),
            key_or_value => $value
        );

        my @update_value_columns_sorted = sort keys %$canon_value;

        alias my @update_value = @$canon_value{ @update_value_columns_sorted };

        my $bucket_id = $self->serialise(\@update_value_columns_sorted);

        push @{ $updates_by_value_column_set{$bucket_id} },
            [ $key, \@update_value ];
    }

    my $update_count = 0;

    # Process each bucket separately
    foreach my $bucket_id (sort keys %updates_by_value_column_set) {
        my $value_columns = $self->deserialise($bucket_id);

        $update_count += $self->multi_update_for_1_key_col_set_1_val_col_set(
            key_columns => $key_columns,
            value_columns => $value_columns,
            updates => $updates_by_value_column_set{$bucket_id}
        );
    }

    return $update_count;
}

=head2 multi_update_for_1_key_col_set_1_val_col_set

    key_columns   => [qw{ kc1 kc2 }], # things you're looking for (WHERE)
    value_columns => [qw{ vc1 vc2 }], # columns you want to SET

    # The updates as a list (not bucketised)
    updates       => [
        [ [ 1,  2 ], [qw{ foo bar }] ],
        [ [ 3,  4 ], [qw{ foo bar }] ],
        [ [ 5,  6 ], [qw{ baz qux }] ],
        [ [ 7,  8 ], [qw{ baz qux }] ],
        [ [ 9, 10 ], [qw{ baz qux }] ],
    ],

=head3 VALUE bucketisation

This implementation bucketises the updates by SET value, then hands off to
multi_update_for_1_key_col_set_bucketised_vals.

Core implementation does essentially this

UPDATE table SET vc1 = 'foo', vc2 = 'bar' WHERE (kc1 = 1 AND kc2 = 2) OR (kc1 = 3 AND kc2 = 4);
UPDATE table SET vc1 = 'baz', vc2 = 'qux' WHERE (kc1 = 5 AND kc2 = 6) OR (kc1 = 7 AND kc2 = 8) OR (kc1 = 9 AND kc2 = 10);

=cut

sub multi_update_for_1_key_col_set_1_val_col_set {
    my ($self,
        $key_columns, $value_columns, $updates
    ) = validated_list(
        \@_,
        key_columns   => { isa => ArrayRef[ Str ] },
        value_columns => { isa => ArrayRef[ Str ] },
        updates       => { isa => ArrayRef }
    );

    # Bucketise by the target values

    # To use a hash for bucketising, we need to flatten the set of values
    # in each update down to a string.
    my %keys_by_value;

    foreach my $update (@$updates) {
        my ($key, $value) = @$update;

        my $bucket_id = $self->serialise($value);

        push @{ $keys_by_value{$bucket_id} }, $key;
    }

    return $self->multi_update_for_1_key_col_set_bucketised_vals(
        key_columns   => $key_columns,
        value_columns => $value_columns,
        updates       => $updates,
        keys_by_value => \%keys_by_value
    );
}

=head2 multi_update_for_1_key_col_set_bucketised_vals

    key_columns   => [qw{ kc1 kc2 }], # things you're looking for (WHERE)
    value_columns => [qw{ vc1 vc2 }], # columns you want to SET

    # The updates as a list (not bucketised)
    updates       => [
        [ [ 1,  2 ], [qw{ foo bar }] ],
        [ [ 3,  4 ], [qw{ foo bar }] ],
        [ [ 5,  6 ], [qw{ baz qux }] ],
        [ [ 7,  8 ], [qw{ baz qux }] ],
        [ [ 9, 10 ], [qw{ baz qux }] ],
    ],

    # The updates bucketised by the values we want to SET
    keys_by_value => {
        'Sfoo,Sbar' => [ [ 1, 2 ], [ 3, 4 ] ],
        'Sbaz,Squx' => [ [ 5, 6 ], [ 7, 8 ], [ 9, 10 ] ],
    }

`updates' and `keys_by_value' are the same data with a different representation

We (the core implementation) use `keys_by_value'; `updates' is passed here
for the benefit of alternative (subclass) implementations (such as the ::Pg
implementation).

This implementation iterates over the buckets (each one representing the
changes for a unique set of things we want to SET) in `keys_by_value', passing
each one to multi_update_for_1_key_col_set_1_val for further processing.

Core implementation does essentially this

UPDATE table SET vc1 = 'foo', vc2 = 'bar' WHERE (kc1 = 1 AND kc2 = 2) OR (kc1 = 3 AND kc2 = 4);
UPDATE table SET vc1 = 'baz', vc2 = 'qux' WHERE (kc1 = 5 AND kc2 = 6) OR (kc1 = 7 AND kc2 = 8) OR (kc1 = 9 AND kc2 = 10);

=cut

sub multi_update_for_1_key_col_set_bucketised_vals {
    my ($self,
        $key_columns, $value_columns, $updates, $keys_by_value
    ) = validated_list(
        \@_,
        key_columns   => { isa => ArrayRef[ Str ] },
        value_columns => { isa => ArrayRef[ Str ] },
        updates       => { isa => ArrayRef },
        keys_by_value => { isa => HashRef }
    );

    my $update_count = 0;

    foreach my $bucket_id (sort keys %$keys_by_value) {
        my $value_array = $self->deserialise($bucket_id);

        my %value_hash;
        alias @value_hash{@$value_columns} = @$value_array;

        $update_count += $self->multi_update_for_1_key_col_set_1_val(
            key_columns => $key_columns,
            value       => \%value_hash,
            keys        => $keys_by_value->{$bucket_id}
        );
    }

    return $update_count;
}

=head2 multi_update_for_1_key_col_set_1_val

Actually issues an UPDATE query

One query is issued, so only one value can be set. But it can be
set for multiple keys

Essentialy, this input

    key_columns => [qw{ kc1 kc2 }],
    value => { p => 'foo', q => 'bar' },
    keys => [ [ 1, 2 ], [ 3, 4 ] ]

issues (approximately) this query

UPDATE table SET p = 'foo', q = 'bar' WHERE (kc1 = 1 AND kc2 = 2) OR (kc1 = 3 AND kc2 = 4)

=cut

sub multi_update_for_1_key_col_set_1_val {
    my ($self,
        $key_columns, $value, $keys
    ) = validated_list(
        \@_,
        key_columns => { isa => ArrayRef[ Str ] },
        value       => { isa => HashRef },
        keys        => { isa => ArrayRef }
    );

    my $dbh = $self->dbh;

    my @value_columns = sort keys %$value;

    my ($value_sql, $value_params) = @{
        $self->tuple_equality_to_sql_and_params(
            tuple        => [ @$value{@value_columns} ],
            column_names => \@value_columns
        )
    };

    my ($where_sql, $where_params) = @{ $self->where_sql_and_params };
    my ($set_sql, $set_params) = @{ $self->set_sql_and_params };

    my ($keys_sql, $keys_params) = @{
        $self->keys_to_where(
            key_columns => $key_columns,
            keys        => $keys
        )
    };

    my $query = join ' ' =>
        'UPDATE', $self->quoted_table_name,
        'SET', join(', ' => @$set_sql, @$value_sql),
        'WHERE', join(' AND ' => @$where_sql, @$keys_sql);

    my $updated_rows_count = $self->prepare_bind_and_execute(
        query => $query,
        parameters => [
            @$set_params, @$value_params, @$where_params, @$keys_params
        ]
    );

    return $updated_rows_count;
}

# should be obvious

sub bind_params {
    my ($self, $sth, $parameters) = validated_list(
        \@_,
        sth          => { },
        parameters   => { isa => ArrayRef },
    );

    my $bpi = 1;

    my $columns_info = $self->table->{columns};

    foreach my $parameter (@$parameters) {
        my ($col_name, $value) = @$parameter;

        confess 'No $col_name' if !defined $col_name;

        my $dbi_type = $columns_info->{$col_name}->{dbi_type};

        if (!defined $dbi_type) {
            confess 'Missing dbi_type for column ' . $col_name;
        }

        $sth->bind_param($bpi++, $value, $dbi_type);
    }

    return;
}

# does what says on tin

sub prepare_bind_and_execute {
    my ($self, $query, $parameters) = validated_list(
        \@_,
        query      => { isa => Str },
        parameters => { isa => ArrayRef, optional => 1 },
    );

    my $sth = $self->dbh->prepare($query);

    if (defined $parameters) {
        $self->bind_params(
            sth        => $sth,
            parameters => $parameters
        );
    }

    if ($ENV{DBIC_TRACE}) {
         my $trace_output = '(DBIx::MultiRow) ' . $query;

        if (defined $parameters) {
            $trace_output .= ': ' . join(', ' => map { pp($_->[1]) } @$parameters);
        }

        say STDERR $trace_output;
    }

    return $sth->execute;
}

# Blind convenience wrapper for dbh->quote_identifier
# 'foo' becomes '"foo"'
# 'foo bar' becomes '"foo bar"'
# So we can cope with strange column names (spaces, capital letters, keywords etc)

sub quote_identifier {
    my $self = shift;

    return $self->dbh->quote_identifier(@_);
}

=head2 multi_delete

table => <DBIxMultiRowTable>,
key_columns => [qw{ col1 col2 }],
deletes => [
  # arrayref keys are interpreted using @$key_columns
  [qw{ foo bar }],

  # hashrefs specify their own columns, and ignore @$key_columns
  { col3 => 'baz', col4 => 'qux' },
  { col1 => 'quux' }
],
where => { company => 'WidgetCo' }

=cut

sub multi_delete {
    my ($self,
        $table, $key_columns, $deletes, $where
    ) = validated_list(
        \@_,
        table       => { isa => DBIxMultiRowTable },
        key_columns => { isa => ArrayRef[ Str ], optional => 1 },
        deletes     => { isa => ArrayRef },
        where       => { isa => HashRef, optional => 1 }
    );

    $self->table($table);
    $self->global_where($where) if defined $where;

    # Bucketise by set of key columns

    # The deletes, but bucketised by the set of key columns.
    # So all updates which use the same set of columns to key off will be in
    # the same bucket.
    my %deletes_by_key_column_set;

    foreach my $delete (@$deletes) {
        my ($canon_key) = $self->canonicalise_key_or_value(
            (defined $key_columns ? (columns => $key_columns) : ()),
            key_or_value => $delete
        );

        my @delete_key_columns_sorted = sort keys %$canon_key;

        alias my @delete_key = @$canon_key{ @delete_key_columns_sorted };

        my $bucket_id = $self->serialise(\@delete_key_columns_sorted);

        push @{ $deletes_by_key_column_set{$bucket_id} }, \@delete_key;
    }

    my $delete_count = 0;

    # Process each bucket separately
    foreach my $bucket_id (sort keys %deletes_by_key_column_set) {
        my $key_columns = $self->deserialise($bucket_id);

        $delete_count += $self->multi_delete_for_1_key_col_set(
            key_columns => $key_columns,
            deletes     => $deletes_by_key_column_set{$bucket_id}
        );
    }

    return $delete_count;
}

=head2 multi_delete_for_1_key_col_set

All keys must be to the same set of columns

Only allowed to use the ArrayRef form --- HashRef ones are not allowed!

key_columns => [qw{
         col1 col2
}],
deletes => [ # = keys
    [qw{ foo  bar }],
    [qw{ baz  qux }]
]

=cut

sub multi_delete_for_1_key_col_set {
    my ($self,
        $key_columns, $deletes
    ) = validated_list(
        \@_,
        key_columns   => { isa => ArrayRef[ Str ] },
        deletes       => { isa => ArrayRef[ ArrayRef ] }
    );

    my ($where_sql, $where_params) = @{ $self->where_sql_and_params };

    my ($keys_sql, $keys_params) = @{
        $self->keys_to_where(
            key_columns => $key_columns,
            keys        => $deletes
        )
    };

    my $query = join ' ' =>
        'DELETE FROM', $self->quoted_table_name,
        'WHERE', join(' AND ' => @$where_sql, @$keys_sql);

    my $deleted_rows_count = $self->prepare_bind_and_execute(
        query      => $query,
        parameters => [ @$where_params, @$keys_params ]
    );

    return $deleted_rows_count;
}

=head2 keys_to_where

Given a list of keys, write the WHERE clause to match any of them

Columns where all values are the same are factorised out.

Given

    keys     => [
        [qw{ foo  bar }, \'NOW()', 'nihao' ],
        [qw{ foo qux quux corge }]
    ],
    key_columns => [qw{ col1 col2   col3  col4  }],

Returns

    [
        [
            # column col1 contains common values ('foo') so gets factorised out
            '"col1" = ?',

            # all other columns contain distinct values, so need to be
            # matched explicitly for each key
            '(("col2" = ? AND "col3" = NOW() AND "col4" = ?) OR ("col2" = ? AND "col3" = ? AND "col4" = ?))'
        ],
        [
            [ 'col1', 'foo' ], [ 'col2', 'bar' ], [ 'col4', 'nihao' ],
            [ 'col2', 'qux' ], [ 'col3', 'quux' ], [ 'col4', 'corge' ]
        ]
    ],

Note: Column factorisation *is* done

=cut

sub keys_to_where {
    my ($self,
        $key_columns, $keys
    ) = validated_list(
        \@_,
        key_columns => { isa => ArrayRef[ Str ] },
        keys        => { isa => ArrayRef[ ArrayRef ] }
    );

    my ($common_tuple_components, $distinct_column_names, $distinct_tuples) = @{
        $self->factorise_tuple_list(
            column_names => $key_columns,
            tuples       => $keys
        )
    }{qw{ common_tuple_components distinct_column_names distinct_tuples }};

    my @where_clauses;
    my @where_params;

    if (%$common_tuple_components) {
        my @column_names = sort keys %$common_tuple_components;
        my @tuple = @$common_tuple_components{@column_names};
        my ($conditions_sql, $conditions_params) = @{
                $self->tuple_equality_to_sql_and_params(
                tuple        => \@tuple,
                column_names => \@column_names
            )
        };

        push @where_clauses, @$conditions_sql;
        push @where_params, @$conditions_params;
    }

    if (@$distinct_column_names && @$distinct_tuples) {
        if (scalar @$distinct_column_names == 1) {
            my $column_name = $distinct_column_names->[0];

            my ($keys_sql, $keys_params) = @{
                $self->elements_to_sql_and_params(
                    elements     => alias [ map { $_->[0] } @$distinct_tuples ],
                    column_names => [ ($column_name) x scalar @$distinct_tuples ]
                )
            };

            push @where_clauses,
                $self->quote_identifier($column_name)
                . ' IN (' . join(', ' => @$keys_sql) . ')';
            push @where_params, @$keys_params;
        } else {
            my ($sql, $params) = @{$self->any_tuple_match_sql_and_params(
                tuples       => $distinct_tuples,
                column_names => $distinct_column_names
            )};

            push @where_clauses, $sql;
            push @where_params, @$params;
        }
    }

    return [ \@where_clauses, \@where_params ];
}

=head2 any_tuple_match_sql_and_params

Given

    tuples     => [
        [qw{ foo  bar }, \'NOW()', 'nihao' ],
        [qw{ foo qux quux corge }]
    ],
    column_names => [qw{ col1 col2   col3  col4  }],

Returns

    [
        '(("col1" = ? AND "col2" = ? AND "col3" = NOW() AND "col4" = ?) OR ("col1" = ? AND "col2" = ? AND "col3" = ? AND "col4" = ?))',
        [
            [ 'col1', 'foo' ], [ 'col2', 'bar' ], [ 'col4', 'nihao' ],
            [ 'col1', 'foo' ], [ 'col2', 'qux' ], [ 'col3', 'quux' ],
            [ 'col4', 'corge' ]
        ]
    ]

Note: No column factorisation is done

=cut

sub any_tuple_match_sql_and_params {
    my ($self, $tuples, $column_names) = validated_list(\@_,
        tuples       => { isa => ArrayRef[ ArrayRef ] },
        column_names => { isa => ArrayRef[ Str ] }
    );

    my @or_clauses;
    my @or_params;

    foreach my $tuple (@$tuples) {
        my ($conditions_sql, $conditions_params) = @{
            $self->tuple_equality_to_sql_and_params(
                tuple        => $tuple,
                column_names => $column_names
            )
        };

        push @or_clauses, '(' . join(' AND ' => @$conditions_sql) . ')';
        push @or_params, @$conditions_params;
    }

    return [
        '(' . join(' OR ' => @or_clauses) . ')',
        \@or_params
    ];
}

=head2 elements_to_sql_and_params

In a listy, vectorised way {
    Generates the required SQL to represent a data value
    (but generating SQL means you often need to generate bind values)
}

Converts a list of data into a list of SQL fragments, and a list of
things which can be munged into bind parameters

Given

    elements     => [qw{ foo  bar }, \'NOW()', 'nihao' ],
    column_names => [qw{ col1 col2   col3  col4  }],

Returns

    [
        [ '?', '?', 'NOW()', '?' ],
        [ [ 'col1', 'foo' ], [ 'col2', 'bar' ], [ 'col4', 'nihao' ] ]
    ]

=cut

sub elements_to_sql_and_params {
    my ($self,
        $elements, $column_names
    ) = validated_list(
        \@_,
        elements     => { isa => ArrayRef },
        column_names => { isa => ArrayRef[ Str ] },
    );

    my @sql;
    my @params;

    for (my $ei = 0; $ei < scalar @$elements; $ei++) {
        alias my $element = $elements->[$ei];
        alias my $column_name = $column_names->[$ei];

        my $element_sql;
        if (ref $element) {
            if (ref $element ne 'SCALAR') {
                confess 'Assertion failure';
            }
            $element_sql = $$element;
        } else {
            $element_sql = '?';
            push @params, alias [ $column_name, $element ];
        }

        push @sql, $element_sql;
    }

    return [ \@sql, \@params ];
}

=head2 tuple_equality_to_sql_and_params

Given

    tuple     => [qw{ foo  bar }, \'NOW()', 'nihao' ],
    column_names => [qw{ col1 col2   col3  col4  }],

Returns

    [
        [ '"col1" = ?', '"col2" = ?', '"col3" = NOW()', '"col4" = ?' ],
        [ [ 'col1', 'foo' ], [ 'col2', 'bar' ], [ 'col4', 'nihao' ] ]
    ]

=cut

sub tuple_equality_to_sql_and_params {
    my ($self,
        $tuple, $column_names
    ) = validated_list(
        \@_,
        tuple        => { isa => ArrayRef },
        column_names => { isa => ArrayRef[ Str ] },
    );

    my ($key_sql, $params) = @{ $self->elements_to_sql_and_params(
        elements => $tuple, column_names => $column_names
    ) };

    my @conditions_sql = map {
        $self->quote_identifier($column_names->[$_]) . ' = ' . $key_sql->[$_]
    } 0 .. $#$column_names;

    return [ \@conditions_sql, $params ];
}

# Convenience method to get the table name which is buried inside the
# table information attribute which is set up on object creation

sub quoted_table_name { $_[0]->table->{quoted_table_name} }

=head2 where_sql_and_params

Gets the SQL WHERE clauses for the "where" parameter supplied to,
e.g. multi_update or multi_delete

Assuming that $self->global_where is

    {
        col1 => 'foo',
        col2 => \'NOW()',
        col3 => 'bar'
    }

Returns

    [
        [ '"col1" = ?', '"col2" = NOW()', '"col3" = ?' ],
        [ [ 'col1', 'foo' ], [ 'col3', 'bar' ] ],
    ]

=cut

sub where_sql_and_params {
    my ($self) = @_;

    my $where = $self->global_where;

    if (!defined $where) {
        return [ [ ], [ ] ];
    }

    my @column_names = sort keys %$where;
    my @matches = @$where{ @column_names };

    return $self->tuple_equality_to_sql_and_params(
        tuple        => \@matches,
        column_names => \@column_names
    );
}

=head2 set_sql_and_params

Gets the SQL SET clauses for the "set" parameter supplied to, e.g. multi_update

Assuming that $self->global_set is

    {
        col1 => 'foo',
        col2 => \'NOW()',
        col3 => 'bar'
    }

Returns

    [
        [ '"col1" = ?', '"col2" = NOW()', '"col3" = ?' ],
        [ [ 'col1', 'foo' ], [ 'col3', 'bar' ] ],
    ]

=cut

sub set_sql_and_params {
    my ($self) = @_;

    my $set = $self->global_set;

    #FIXME: from here to bottom same as where_sql_and_params. factorise?

    if (!defined $set) {
        return [ [ ], [ ] ];
    }

    my @column_names = sort keys %$set;
    my @set_values = @$set{ @column_names };

    return $self->tuple_equality_to_sql_and_params(
        tuple        => \@set_values,
        column_names => \@column_names
    );
}

=head2 factorise_tuple_list

Given

    column_names => [qw{
             col1 col2 col3
    }],
    tuples => [
        [qw{ foo  bar  baz }],
        [qw{ foo  bar  qux }],
        [qw{ foo  bat  quy }],
    ]

Returns

    {
        common_tuple_components => { col1 => 'foo' }, # for our where clause
        distinct_column_names   => [qw{
                 col2 col3
        }],
        distinct_tuples         => [
            [qw{ bar  baz }],
            [qw{ bar  qux }],
            [qw{ bat  quy }]
        ]
    }

preserving the order of the columns in the list distinct_column_names
and distinct_tuples.

=cut

sub factorise_tuple_list {
    my ($self, $column_names, $tuples) = validated_list(
        \@_,
        column_names => { isa => ArrayRef[ Str ] },
        tuples       => { isa => ArrayRef[ ArrayRef ] }
    );

    my $tuple_component_distinctness = $self->tuple_list_component_distinctness(
        tuples => $tuples
    );

    my @distinct_teis = grep { $tuple_component_distinctness->[$_] } 0 .. $#$column_names;
    my @distinct_column_names = @$column_names[@distinct_teis];
    my @distinct_tuples =
        map {
            alias [ @$_[@distinct_teis] ]
        } @$tuples;

    my %common_tuple_components;
    if (@$tuples) {
        my @common_teis = grep { !$tuple_component_distinctness->[$_] } 0 .. $#$column_names;
        my @common_column_names = @$column_names[@common_teis];

        @common_tuple_components{@common_column_names} =
            @{ $tuples->[0] }[ @common_teis ];
    }

    return {
        common_tuple_components => \%common_tuple_components,
        distinct_column_names   => \@distinct_column_names,
        distinct_tuples         => \@distinct_tuples
    };
}

=head2 tuple_list_component_distinctness

A wrapper of tuple_list_component_distinctness_generic passing in a
particular comparer. This comparer is a bit like a scalar eq comparison
but will also return true if the arguments are references to strings
which are equal. It will also cope with undefs (which scalar eq does not).

=cut

sub tuple_list_component_distinctness {
    my ($self, $tuples, $comparer) = validated_list(
        \@_,
        tuples   => { isa => ArrayRef[ ArrayRef [ Undef | Str | ScalarRef ] ] },
        comparer => {
            isa      => CodeRef,
            optional => 1,
            default  => sub {
                alias my ($p, $q) = @_;

                if ((defined $p) && (defined $q)) {
                    if ((!ref $p) && (!ref $q)) {
                        return ($p eq $q) ? 0 : 1;
                    } elsif ((ref $p) && (ref $q)) {
                        return ($$p eq $$q) ? 0 : 1;
                    } else {
                        return 1;
                    }
                } elsif ((!defined $p) && (!defined $q)) {
                    return 0;
                } else {
                    return 1;
                }
            }
        }
    );

    return $self->tuple_list_component_distinctness_generic(
        tuples   => $tuples,
        comparer => $comparer
    );
}

=head2 tuple_list_component_distinctness_generic

Given a array of tuples, return an array of booleans
where each boolean indicates whether that column in the
input tuples contains more than one value.

e.g. given

  qw{foo bar baz}
  qw{foo bar qux}
  qw{foo bat quy}

Returns

  [ 0, 1, 1 ]

=cut

sub tuple_list_component_distinctness_generic {
    my ($self, $tuples, $comparer) = validated_list(
        \@_,
        tuples   => { isa => ArrayRef[ ArrayRef ] },
        comparer => {
            isa      => CodeRef,
            optional => 1,
            default  => sub { return $_[0] cmp $_[1] }
        }
    );

    return [ ] if !@$tuples;

    my $min_tuple_size = my $max_tuple_size = scalar @{ $tuples->[0] };

    for (my $ti = 1; $ti < scalar @$tuples; $ti++) {
        alias my $tuple = $tuples->[$ti];

        if (scalar @$tuple > $max_tuple_size) {
            $max_tuple_size = scalar @$tuple;
        }

        if (scalar @$tuple < $min_tuple_size) {
            $min_tuple_size = scalar @$tuple;
        }
    }

    if ($min_tuple_size == 0) {
        return [ (1) x $max_tuple_size ];
    }

    my %common_indexes_set;
    @common_indexes_set{ 0 .. $min_tuple_size - 1 } = ();

    # Array alias for optimal array lookup performance in the inner loop
    alias my @common = @{ $tuples->[0] };

    TUPLE: for (my $ti = 1; $ti < scalar @$tuples; $ti++) {
        # Array alias for optimal array lookup performance in the inner loop
        alias my @tuple = @{ $tuples->[$ti] };

        foreach my $tei (keys %common_indexes_set) {
            if ($comparer->($tuple[$tei], $common[$tei])) {
                delete $common_indexes_set{$tei};

                last TUPLE if !%common_indexes_set;
            }
        }
    }

    my %distinct_indexes;
    @distinct_indexes{ 0 .. $max_tuple_size - 1 } = ();

    delete @distinct_indexes{ keys %common_indexes_set };

    return [
        map { exists $distinct_indexes{$_} ? 1 : 0 } 0 .. $max_tuple_size - 1
    ];
}

=head2 serialise

Serialise an array into a string.

Used for creating bucket IDs from tuples (either keys or values)

Implemented own one because we want to be able to serialise ScalarRef in a
specific way.

Essentially, items are joined with commas.  There is mitigation so that
it can cope with commas in the input array.

So

    ( qw{ foo bar }, \'baz', 'p,q', 'r=s', undef )

becomes

    'Sfoo,Sbar,Rbaz,Sp=cq,Sr=es,-'

=cut

sub serialise {
    my ($self, $array) = @_;

    my $encode = sub {
        my $x = shift;

        $x =~ s{=}{=e}g;
        $x =~ s{,}{=c}g;

        return $x;
    };

    return join ',' =>
        map {
            my $element = $_;
            my $ser_elem;

            if (defined $element) {
                if (ref $element) {
                    $ser_elem = 'R' . $encode->($$element);
                } else {
                    $ser_elem = 'S' . $encode->($element);
                }
            } else {
                $ser_elem = '-';
            }

            $ser_elem;
        } @$array;
}

=head2

Opposite of serialise

=cut

sub deserialise {
    my ($self, $serialised) = @_;

    my $decode = sub {
        my $x = shift;

        $x =~ s{=c}{,}g;
        $x =~ s{=e}{=}g;

        return $x;
    };

    return [
        map {
            my $ser_elem = $_;

            $ser_elem =~ m{ \A (?<prefix>.) (?<value>.*) \z }sx;

            my $element;

            if ($+{prefix} eq 'S') {
                $element = $decode->($+{value});
            } elsif ($+{prefix} eq 'R') {
                $element = \$decode->($+{value});
            } elsif ($+{prefix} eq '-') {
                $element = undef;
            } else {
                confess 'Unknown serialisation prefix';
            }

            $element;
        } split(',', $serialised, -1)
    ];
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
