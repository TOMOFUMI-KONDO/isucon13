use v5.38;
use utf8;
use lib 'lib';

# あとで消す

use Test2::V0;
use Plack::Test;
use HTTP::Request::Common;
use HTTP::Status qw(:constants);
use HTTP::Cookies;
use Cpanel::JSON::XS ();
use Cpanel::JSON::XS::Type;
use Encode ();

my $app = do './app.psgi';

sub decode_json {
    state $json = Cpanel::JSON::XS->new()->ascii(0)->convert_blessed;
    return $json->decode(@_);
}

sub with_json_request($req, $data) {
    state $json = Cpanel::JSON::XS->new->utf8;
    my $encocded_json = Encode::encode_utf8($json->encode($data));

    $req->header('Content-Type' => 'application/json; charset=utf-8');
    $req->header('Content-Length' => length $encocded_json);
    $req->content($encocded_json);

    return $req;
}

sub login_default($cb, $req) {
    my $login_req = POST "/api/login";
    with_json_request($login_req, {
        name     => 'test001',
        password => 'test001',
    });
    my $login_res = $cb->($login_req);
    my $cookie = $login_res->header('Set-Cookie');

    $req->header('Cookie' => $cookie);
    return $req;
}


subtest 'POST /api/initialize' => sub {
    test_psgi $app, sub ($cb){
        my $res = $cb->(POST "/api/initialize");
        is decode_json($res->content), {
            advertise_level => 10,
            language        => 'perl',
        };
    };
};

subtest 'GET /api/tag' => sub {
    test_psgi $app, sub ($cb){
        my $res = $cb->(GET "/api/tag");
        is $res->code, HTTP_OK;

        my $tags = decode_json($res->content, my $decode_type);
        is $decode_type, {
            tags => array {
                all_items {
                    id   => JSON_TYPE_INT,
                    name => JSON_TYPE_STRING,
                };
                etc;
            }
        };
    };
};

subtest 'GET /api/user/:username/theme' => sub {
    test_psgi $app, sub ($cb) {
        my $req = GET "/api/user/test001/theme";
        login_default($cb, $req);

        my $res = $cb->($req);
        is $res->code, HTTP_OK;

        is decode_json($res->content), {
            id        => 1,
            dark_mode => 0,
        };
    };
};

subtest 'POST /api/livestream/reservation' => sub {
    test_psgi $app, sub ($cb) {
        my $req = POST "/api/livestream/reservation";
        login_default($cb, $req);

        with_json_request($req, {
            tags => [43], # DIY
            title => '月曜大工',
            description => 'キーボードをつくります',
            collaborators => [],
            start_at    => 1714521600, # 2024/05/01 UTC
            end_at      => 1717200000, # 2024/06/01 UTC
        });

        my $res = $cb->($req);
        is ($res->code, HTTP_CREATED) or diag $res->content;

        is decode_json($res->content), hash {
            field id => 2;
            etc;
        };
    };
};

subtest 'POST /api/login' => sub {
    test_psgi $app, sub ($cb) {

        my $req = POST "/api/login";
        with_json_request($req, {
            name     => 'test001',
            password => 'test001',
        });

        my $res = $cb->($req);
        is $res->code, HTTP_OK;
        is $res->content, '';
    };
};

done_testing;
