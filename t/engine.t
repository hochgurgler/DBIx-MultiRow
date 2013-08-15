#!perl

use Test::More;
use Test::Deep;

use_ok('DBIx::MultiRow::Engine');

my $mre = DBIx::MultiRow::Engine->new();

is(
    $mre->serialise([ qw{ foo bar }, \'baz', 'p,q', 'r=s', undef ]),
    'Sfoo,Sbar,Rbaz,Sp=cq,Sr=es,-',
    'serialise'
);

cmp_deeply(
    $mre->deserialise(
        'Sfoo,Sbar,Rbaz,Sp=cq,Sr=es,-'
    ),
    [ qw{ foo bar }, \'baz', 'p,q', 'r=s', undef ],
    'deserialise'
);

cmp_deeply(
    $mre->tuple_list_component_distinctness_generic(
        tuples => [[qw/foo bar baz/], [qw/foo bar qux/], [qw/foo bat quy/]]
    ),
    [0,1,1],
    'tuple_list_component_distinctness_generic'
);

cmp_deeply(
    $mre->tuple_list_component_distinctness(
        tuples => [
        [qw/foo bar baz/, \'NOW()', undef, undef],
        [qw/foo bar qux/,\'NOW()', undef, 1],
        [qw/foo bat quy/, \'NOW()', undef, 1]
    ]),
    [0,1,1,0,0,1],
    'tuple_list_component_distinctness'
);

cmp_deeply(
    $mre->factorise_tuple_list(
        column_names => [qw{
            col1 col2 col3
        }],
        tuples => [
            [qw{ foo  bar  baz }],
            [qw{ foo  bar  qux }],
            [qw{ foo  bat  quy }],
        ]
    ),
    {
        common_tuple_components => { col1 => 'foo' },
        distinct_column_names   => [qw{
                 col2 col3
        }],
        distinct_tuples         => [
            [qw{ bar  baz }],
            [qw{ bar  qux }],
            [qw{ bat  quy }]
        ]
    },
    'factorise_tuple_list'
);

cmp_deeply(
    $mre->elements_to_sql_and_params(
        elements => [qw{ foo  bar }, \'NOW()', 'nihao'],
        column_names => [ qw{ col1 col2   col3  col4  }],
    ),
    [
        [ '?', '?', 'NOW()', '?' ],
        [ [ 'col1', 'foo' ], [ 'col2', 'bar' ], [ 'col4', 'nihao' ] ]
    ],
    'elements_to_sql_and_params'
);

package TestDB {
    use Moose;

    sub quote_identifier { return qq{"$_[1]"} }
}

$mre->dbh(TestDB->new);

cmp_deeply(
    $mre->tuple_equality_to_sql_and_params(
        tuple => [qw{ foo  bar }, \'NOW()', 'nihao'],
        column_names => [ qw{ col1 col2   col3  col4  }],
    ),
    [
        [ '"col1" = ?', '"col2" = ?', '"col3" = NOW()', '"col4" = ?' ],
        [ [ 'col1', 'foo' ], [ 'col2', 'bar' ], [ 'col4', 'nihao' ] ]
    ],
    'tuple_equality_to_sql_and_params'
);

cmp_deeply(
    $mre->any_tuple_match_sql_and_params(
        tuples => [
            [qw{ foo  bar }, \'NOW()', 'nihao' ],
            [qw{ foo qux quux corge }]
        ],
        column_names => [ qw{ col1 col2   col3  col4  }],
    ),
    [
        '(("col1" = ? AND "col2" = ? AND "col3" = NOW() AND "col4" = ?) OR ("col1" = ? AND "col2" = ? AND "col3" = ? AND "col4" = ?))',
        [
            [ 'col1', 'foo' ], [ 'col2', 'bar' ], [ 'col4', 'nihao' ],
            [ 'col1', 'foo' ], [ 'col2', 'qux' ], [ 'col3', 'quux' ],
            [ 'col4', 'corge' ]
        ]
    ],
    'any_tuple_match_sql_and_params'
);

my $ktwr = $mre->keys_to_where(
        keys => [
            [qw{ foo  bar }, \'NOW()', 'nihao' ],
            [qw{ foo qux quux corge }]
        ],
        key_columns => [ qw{ col1 col2   col3  col4  }],
    );

cmp_deeply(
    $ktwr,
    [
        [
            '"col1" = ?',
            '(("col2" = ? AND "col3" = NOW() AND "col4" = ?) OR ("col2" = ? AND "col3" = ? AND "col4" = ?))'
        ],
        [
            [ 'col1', 'foo' ], [ 'col2', 'bar' ], [ 'col4', 'nihao' ],
            [ 'col2', 'qux' ], [ 'col3', 'quux' ], [ 'col4', 'corge' ]
        ]
    ],
    'keys_to_where'
);

$mre->global_set({
    col1 => 'foo',
    col2 => \'NOW()',
    col3 => 'bar'
});

cmp_deeply(
    $mre->set_sql_and_params,
    [
        [ '"col1" = ?', '"col2" = NOW()', '"col3" = ?' ],
        [ [ 'col1', 'foo' ], [ 'col3', 'bar' ] ],
    ],
    'set_sql_and_params'
);

$mre->global_where({
    col1 => 'foo',
    col2 => \'NOW()',
    col3 => 'bar'
});

cmp_deeply(
    $mre->where_sql_and_params,
    [
        [ '"col1" = ?', '"col2" = NOW()', '"col3" = ?' ],
        [ [ 'col1', 'foo' ], [ 'col3', 'bar' ] ],
    ],
    'where_sql_and_params'
);

cmp_deeply(
    $mre->canonicalise_key_or_value(columns => [qw{ c1 }], key_or_value => 'foo'),
    { c1 => 'foo' },
    'canonicalise_key_or_value - key_or_value a scalar'
);

cmp_deeply(
    $mre->canonicalise_key_or_value(
        columns      => [qw{ c1 c2 }],
        key_or_value => [qw{ foo bar }]
    ),
    { c1 => 'foo', c2 => 'bar' },
    'canonicalise_key_or_value - key_or_value an arrayref'
);

cmp_deeply(
    $mre->canonicalise_key_or_value(
        columns      => [qw{ quack bark oink }],
        key_or_value => { c1 => 'foo', c2 => 'bar' }
    ),
    { c1 => 'foo', c2 => 'bar' },
    'canonicalise_key_or_value - key_or_value a hashref'
);

done_testing;

__END__

=head1 AUTHOR

William Blunn <william.blunn@net-a-porter.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2013 by Net-a-porter.com.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut
