package DBIx::MultiRow::Engine::DB::Pg;

# "use Moose;" does "use strict;" and "use warnings;" for us

use Moose;

extends 'DBIx::MultiRow::Engine';

use MooseX::Params::Validate;
use MooseX::Types::Moose qw{ ArrayRef HashRef Str};
use Const::Fast;
use Data::Alias;

use DBIx::MultiRow 'DBIxMultiRowTable';

# We might think that we could optimise
# _multi_update_for_1_key_col_set_1_val_col_set for PostgreSQL by making it
# compose UPDATE ... FROM ... VALUES but unfortunately in that case PostgreSQL
# doesn't seem to want to use the relevant index on the target table, and so
# while it will probably be lovely and fast to begin with, it will probably
# not scale well.

# Similarly we might think that we could optimise
# _multi_delete_for_1_key_col_set for PostgreSQL by making it compose
# DELETE FROM ... USING ... VALUES but unfortunately in that case PostgreSQL
# doesn't seem to want to use the relevant index on the target table, and so
# while it will probably be lovely and fast to begin with, it will probably
# not scale well.

my $tmp_table_index = 1;

# PostgreSQL optimised bulk update requires four queries:
# 1. CREATE TEMPORARY TABLE ( ... ) ON COMMIT DROP
# 2. INSERT INTO ... VALUES ...
# 3. UPDATE ... SET ... FROM ... WHERE ...
# 4. DROP TABLE ...
const my $optimised_bulk_update_query_count => 4;

=head2 multi_update_for_1_key_col_set_bucketised_vals

Overrides the superclass method.

The superclass ordinarily issues an UPDATE for each distinct SET clause.

However, another way of effecting heterogeneous updates is to put them
into a temporary table, join the temporary table with the target table,
then use the SET clause to copy values from the temporary table to the
target table.

The SQL syntax required to do "UPDATE with join" tends to be different
for different databases, so we need database-specific code to do it.

This method implements this approach for PostgreSQL, which for the update
uses syntax like this:

    UPDATE table
    SET
        vc1 = tmptable.vc1,
        vc2 = tmptable.vc2,
        vc3 = <some_global_value>
    FROM tmptable
    WHERE
            tmptable.kc1 = table.kc1
        AND tmptable.kc2 = table.kc1
        AND table.kc3 = <some_global_key>

This approach requires at least four SQL statements:

    1. Create temporary table
    2. Populate temporary table
    3. Do update
    4. Drop temporary table

This method checks the number of distinct target value tuples
(indicating the number of UPDATE statements required for the core approach)
and only uses the approach described above if it would result in
fewer queries than the core approach.

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

    my $distinct_value_tuples = keys %$keys_by_value;

    # There is probably not much point using this optimised approach where
    # the required updates can be accomplished using fewer queries (for
    # example if there are only three distinct value tuples).
    if ($distinct_value_tuples <= $optimised_bulk_update_query_count) {
        return $self->SUPER::multi_update_for_1_key_col_set_bucketised_vals(
            key_columns   => $key_columns,
            value_columns => $value_columns,
            updates       => $updates,
            keys_by_value => $keys_by_value
        );
    }

    # Detect common key components
    my $key_component_distinctness = $self->tuple_list_component_distinctness(
        tuples => [ map { $_->[0] } @$updates ]
    );
    my @distinct_key_indexes = grep {  $key_component_distinctness->[$_] } 0 .. $#$key_columns;
    my @common_key_indexes   = grep { !$key_component_distinctness->[$_] } 0 .. $#$key_columns;

    if (!@distinct_key_indexes) {
        # No distinct keys(!) Too hard.  Let the default implementation handle
        # it.
        return $self->SUPER::multi_update_for_1_key_col_set_bucketised_vals(
            key_columns   => $key_columns,
            value_columns => $value_columns,
            updates       => $updates,
            keys_by_value => $keys_by_value
        );
    }

    my @distinct_key_columns = @{ $key_columns }[ @distinct_key_indexes ];

    my $ckc_sql;
    my $ckc_params;

    if (@common_key_indexes) {
        my @column_names = @$key_columns[@common_key_indexes];

        ($ckc_sql, $ckc_params) = @{
                $self->tuple_equality_to_sql_and_params(
                tuple        => [ @{ $updates->[0]->[0] }[ @common_key_indexes ] ],
                column_names => [ @{ $key_columns       }[ @common_key_indexes ] ]
            )
        };
    } else {
        $ckc_sql = [];
        $ckc_params = [];
    }

    # Create temporary table
    my $tmp_table_name = "_tmp_dbix_multirow_pg_${tmp_table_index}";
    $tmp_table_index++;

    my $columns_info = $self->table->{columns};

    my $create_tmp_table_query = join ' ' =>
        'CREATE TEMPORARY TABLE', $tmp_table_name, '(' . join(', ' =>
            map {
                $self->quote_identifier($_) . ' ' . $columns_info->{$_}->{sql_type}
            } @distinct_key_columns, @$value_columns
        ) . ')', 'ON COMMIT DROP';

    $self->prepare_bind_and_execute( query => $create_tmp_table_query );

    # Populate temporary table with required updates
    my @values_sql;
    my @values_params;

    foreach my $update (@$updates) {
        my @value_sql;
        my @value_params;

        my ($k_sql, $k_params) = @{
            $self->elements_to_sql_and_params(
                elements     => alias [ @{ $update->[0] }[ @distinct_key_indexes ] ],
                column_names => \@distinct_key_columns
            )
        };
        alias push @value_sql, @$k_sql;
        alias push @value_params, @$k_params;

        my ($v_sql, $v_params) = @{
            $self->elements_to_sql_and_params(
                elements     => $update->[1],
                column_names => $value_columns
            )
        };
        alias push @value_sql, @$v_sql;
        alias push @value_params, @$v_params;

        push @values_sql, '(' . join(',' => @value_sql) . ')';
        alias push @values_params, @value_params;
    }

    my $insert_query = join ' ' =>
        'INSERT INTO', $tmp_table_name, '(' . join(', ' =>
            map { $self->quote_identifier($_) } @distinct_key_columns, @$value_columns
        ) . ')',
        'VALUES', join(',' => @values_sql);

    $self->prepare_bind_and_execute(
        query      => $insert_query,
        parameters => \@values_params
    );

    # Update target table using data from temporary table
    my $tmp_table_alias = 'x';

    if (
        ($self->quoted_table_name eq $tmp_table_alias)
        || ($self->quoted_table_name eq $self->quote_identifier($tmp_table_alias))
    ) {
        $tmp_table_alias = 'y';
    }

    my ($where_sql, $where_params) = @{ $self->where_sql_and_params };
    my ($set_sql, $set_params) = @{ $self->set_sql_and_params };

    my $update_query = join ' ' =>
        'UPDATE', $self->quoted_table_name,
        'SET', join(', ' =>
            @$set_sql,
            (map {
                my $quoted_column_name = $self->quote_identifier($_);

                $quoted_column_name . ' = ' . "${tmp_table_alias}.$quoted_column_name";
            } @$value_columns)
        ),
        'FROM', $tmp_table_name, $tmp_table_alias,
        'WHERE', join(' AND ' =>
            @$where_sql,
            @$ckc_sql,
            (map {
                my $quoted_column_name = $self->quote_identifier($_);

                "${tmp_table_alias}.${quoted_column_name} = "
                . $self->quoted_table_name . ".${quoted_column_name}";
            } @distinct_key_columns)
        );

    my $rows_count =
        $self->prepare_bind_and_execute(
            query      => $update_query,
            parameters => [ @$set_params, @$where_params, @$ckc_params ]
        );

    # Drop temporary table
    my $drop_tmp_table_query = join ' ' => 'DROP TABLE', $tmp_table_name;

    $self->prepare_bind_and_execute( query => $drop_tmp_table_query );

    return $rows_count;
}

=head2 any_tuple_match_sql_and_params

Overrides the superclass method.

The superclass will generate SQL like

    (kc1 = 'foo' AND kc2 = 'bar') OR (kc1 = 'baz' AND kc2 = 'qux') OR ...

In PostgreSQL we can use IN (...) with tuples, so this method generates SQL
like

    (kc1, kc2) IN (('foo', 'bar'), ('baz', 'qux'))

Empirically, the query plan in both cases seems to be the same.

However using IN generates significantly smaller SQL.  With large numbers
of tuples this could be significant.

=cut

sub any_tuple_match_sql_and_params {
    my ($self, $tuples, $column_names) = validated_list(\@_,
        tuples       => { isa => ArrayRef[ ArrayRef ] },
        column_names => { isa => ArrayRef[ Str ] }
    );

    my @tuples_sql;
    my @params;

    foreach my $tuple (@$tuples) {
        my ($sql, $params) = @{$self->elements_to_sql_and_params(
            elements     => $tuple,
            column_names => $column_names
        )};

        push @tuples_sql, '(' . join(', ' => @$sql) . ')';
        push @params, @$params;
    }

    my $sql = '(' . join(' ' =>
        '(' . join(', ' => map { $self->quote_identifier($_) } @$column_names) . ')',
        'IN',
        '(' . join(', ' => @tuples_sql) . ')'
    ) . ')';

    return [ $sql, \@params ];
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
