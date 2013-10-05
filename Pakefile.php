<?php

pake_task('import_schools');
pake_task('import_people');
pake_task('import_vuz');

pake_desc('import');
pake_task('import', 'import_schools', 'import_people', 'import_vuz');

define("data_root", __DIR__.'/data');

/**
 * Import generic school data
 */
function run_import_schools()
{
    $school_file = data_root.'/CodsOU-2.csv';
    $specs = ["biology", "chemistry", "english", "german", "history", "inform", "math", "obshestvo", "physics", "russian"];

    $specs_data = [];
    foreach ($specs as $spec) {
        $specs_data[$spec] = read_csv_file(data_root.'/arch_'.$spec.'_2013_schools_xls.csv');
    }

    $collection = mongo_connect()->selectCollection('schools');

    pake_echo_action('mongo-', 'schools');
    $collection->drop();

    pake_echo_action('mongo+', 'schools');
    foreach (read_csv_file($school_file) as $school_line) {
        if ($school_line[1] == 'SchoolName') {
            continue;
        }

        $node = new stdClass();
        $node->code = $school_line[0];
        $node->schoolname = $school_line[1];
        $node->type = $school_line[2];
        $node->level = $school_line[3];
        $node->organization = $school_line[4];
        $node->spec = $school_line[5];
        $node->subject = $school_line[6];
        $node->shortname = $school_line[7];
        $node->city = $school_line[8];
        $node->citytype = $school_line[9];
        $node->region = $school_line[10];
        $node->address = $school_line[11];

        $node->results = [];
        foreach ($specs as $spec) {
            if (null === $row = find_row($specs_data[$spec], $school_line[0])) {
                $row = array_fill(0, 11, 0);
            }

            $node->results[$spec] = [
                "number_of_contestants" => $row[1],
                "average_result" => $row[3],
                "number_of_failures" => $row[4],
                "number_of_males" => $row[7],
                "average_male_result" => $row[8],
                "number_of_females" => $row[9],
                "average_female_result" => $row[10],
            ];
        }

        $collection->insert($node);
        echo '.';
    }
    echo "\n";
}

/**
 * Import Student exam grades data
 */
function run_import_people()
{
    $collection = mongo_connect()->selectCollection('people');

    pake_echo_action('mongo-', 'people');
    $collection->drop();

    $people = read_csv_file(data_root.'/Students-result 2.csv');

    pake_echo_action('mongo+', 'people');
    foreach ($people as $person) {
        if ($person[0] == 'ФИО') {
            continue;
        }

        $obj = new stdClass();
        $obj->familyname = $person[4];
        $obj->firstname = $person[5];
        $obj->patronym = $person[6];
        $obj->school_learned = $person[2];
        $obj->class_name = $person[3];
        $obj->school_exam = $person[1];

        $obj->results = new stdClass();
        $obj->results->russian = $person[7] ?: 0;
        $obj->results->math = $person[8] ?: 0;
        $obj->results->physics = $person[9] ?: 0;
        $obj->results->informatics = $person[10] ?: 0;
        $obj->results->biology = $person[11] ?: 0;
        $obj->results->chemistry = $person[12] ?: 0;
        $obj->results->english = $person[13] ?: 0;
        $obj->results->french = $person[14] ?: 0;
        $obj->results->geography = $person[15] ?: 0;
        $obj->results->german = $person[16] ?: 0;
        $obj->results->history = $person[17] ?: 0;
        $obj->results->literature = $person[18] ?: 0;
        $obj->results->obshestv = $person[19] ?: 0;

        $collection->insert($obj);
        echo '.';
    }
    echo "\n";

}

/**
 * Import NSMU data
 */
function run_import_vuz()
{
    $collection = mongo_connect()->selectCollection('highschool');

    pake_echo_action('mongo-', 'highschool');
    $collection->drop();

    $nsmu = read_tsv_file(data_root.'/nsmu.tsv');

    pake_echo_action('mongo+', 'highschool');

    foreach ($nsmu as $row) {
        $name = explode(' ', $row[0]);

        $obj = new stdClass();
        $obj->highschool = 'nsmu';
        $obj->familyname = $name[0];
        $obj->firstname = $name[1];
        $obj->patronym = $name[2];

        $obj->specialty = $row[1];

        $obj->results = new stdClass();
        $obj->results->total = $row[7];
        $obj->results->russian = $row[3] ?: 0;
        $obj->results->math = $row[4] ?: 0;
        $obj->results->chemistry= $row[5] ?: 0;
        $obj->results->biology = $row[6] ?: 0;

        $collection->insert($obj);
        echo '.';
    }
    echo "\n";
}

function run_import(){}

// ===============
// helpers below
// ===============

function read_csv_file($filename)
{
    $results = [];
    $fp = fopen($filename, 'r');
    while ($line = fgetcsv($fp, 0, ';', '"')) {
        $results[] = $line;
    }
    fclose($fp);

    return $results;
}

function read_tsv_file($filename)
{
    $results = [];
    $fp = fopen($filename, 'r');
    while ($line = fgetcsv($fp, 0, "\t")) {
        $results[] = $line;
    }
    fclose($fp);

    return $results;
}


function find_row(array $data, $value, $column = 0)
{
    foreach ($data as $row) {
        if ($row[$column] == $value) {
            return $row;
        }
    }

    return null;
}

/**
 * @return MongoDB
 */
function mongo_connect()
{
    static $m = null;

    if (null === $m) {
        pake_echo_action('mongo', 'Connecting');

        if (file_exists(__DIR__.'/config.yaml')) {
            $uri = pakeYaml::load(__DIR__.'/config.yaml')['mongo'];
            $m = new MongoClient($uri);
        } else {
            $m = new MongoClient();
        }
    }

    return $m->selectDB('ege');
}
