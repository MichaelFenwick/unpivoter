import 'dart:io';
import 'package:args/args.dart';
import 'package:args/command_runner.dart';
import 'package:grizzly_io/grizzly_io.dart' as grizzly_io;

String programDescription = '''
This command line utility will process a provided tsv or csv file, along with a predefined set of columns to be used as "keys." Each line in the input file will be transformed into multiple lines containing the values of all fields in the key columns, followed by two more values, the name of one of the non-key columns and that column's value. In this way, a file with n column and m key columns will have each line transformed into n-m key new lines, each containing m+2 fields.

If the input file has a header row (specified with the -H or --headers flag) then that row will be read and its values used to identify the key columns and as values for the non-key column names. If there is no header row, then zero-indexed integers will be used as column "names" instead. The output will contain a header row only if the input file did as well.

By default, the unpivoted data is output to stdout, from which it can be piped into another utility or written to a file. A mechanism is also provided via the -i or --inline flag to have the output written over the original input file. In either case, the output will be formatted in the same manner as the input (csv or tsv).
''';

typedef Encoder = String Function(Iterable<Iterable<dynamic>>);
typedef Parser = grizzly_io.Table Function(String);

String encodeCsv(Iterable<Iterable<dynamic>> data) => grizzly_io.encodeCsv(reifyNestedIterables(data));

String encodeTsv(Iterable<Iterable<dynamic>> data) {
  data =
      data.map((Iterable<dynamic> row) => row.map((dynamic element) => (element as String).replaceAll('\t', '\\\t')));
  return grizzly_io.encodeCsv(reifyNestedIterables(data), fieldSep: '\t', textSep: '');
}

grizzly_io.Table parseCsvWithHeaders(String data) => grizzly_io.parseLCsv(data);

grizzly_io.Table parseCsvWithoutHeaders(String data) => grizzly_io.Table.from(grizzly_io.parseCsv(data));

grizzly_io.Table parseTsvWithHeaders(String data) => grizzly_io.parseLTsv(data);

grizzly_io.Table parseTsvWithoutHeaders(String data) => grizzly_io.Table.from(grizzly_io.parseTsv(data));

void main(List<String> arguments) {
  ArgParser argParser = ArgParser();
  argParser
    ..addMultiOption('keys',
        abbr: 'k',
        help:
            'A comma separated list of keys (header names for files with a header row, or column indices for files without a header row) which will be used as a conserved "label" for each row of data. The values of these columns will be used for each new row, alongside a single value from one of the non-key columns.')
    ..addOption('file', abbr: 'f', help: 'The path of the file to be processed.', mandatory: true)
    ..addOption('mode',
        allowed: ['csv', 'tsv'],
        abbr: 'm',
        help:
            'The format of the file being processed. This only needs to be specified if the file\'s extension does not match one of the allowed mode values.',
        allowedHelp: {
          'csv': 'Comma separated value files. Files need to conform to https://datatracker.ietf.org/doc/html/rfc4180',
          'tsv': 'Tab separated value files. Fields with tabs in them need to have the tabs escaped a \'\\\'',
        })
    ..addFlag('headers', abbr: 'H', help: 'If used, indicates that the first row of the file is a header row.')
    ..addFlag('inline',
        abbr: 'i', help: 'If used, will update the file in place rather than outputting the formatted data to stdout.')
    ..addFlag('help', abbr: 'h', help: 'View this help text.', negatable: true);

  void outputHelpText() {
    stdout.writeln(programDescription);
    stdout.writeln(argParser.usage);
    exit(0);
  }

  try {
    ArgResults argResults = argParser.parse(arguments);

    if (argResults['help']) {
      outputHelpText();
    }

    if (argResults['file'] == null) {
      stderr.writeln('You must specify a file to process using the --file or -f argument.');
      exit(64);
    }

    if (argResults['keys'] == []) {
      stderr.writeln('You must specify one or more column to use as a key for the unpivoting.');
      exit(64);
    }

    File file = File(argResults['file']);
    List<String> keys = argResults['keys'];
    String mode = argResults['mode'] ?? file.path.substring(file.path.length - 3);
    bool updateInline = argResults['inline'];
    bool hasHeaders = argResults['headers'];

    String fileContents = file.readAsStringSync();

    grizzly_io.Table data;
    Encoder encoder;
    Parser parser;
    switch (mode) {
      case 'csv':
        parser = hasHeaders ? parseCsvWithHeaders : parseCsvWithoutHeaders;
        data = parser(fileContents);
        encoder = encodeCsv;
        break;
      case 'tsv':
        parser = hasHeaders ? parseTsvWithHeaders : parseTsvWithoutHeaders;
        data = parser(fileContents);
        encoder = encodeTsv;
        break;
      default:
        print('Invalid file format $mode. Use the --mode or -m argument to specify the format of your file.');
        exit(64);
    }

    Iterable<Iterable<dynamic>> unpivotedData = unpivotData(data, keys);

    if (hasHeaders) {
      List<String> headers = keys + ['key', 'value'];
      Iterable<dynamic> headerIterable = Iterable.generate(headers.length, (int i) => headers[i]);
      unpivotedData = Iterable.generate(1, (_) => headerIterable).followedBy(unpivotedData);
    }

    String encodedResult = encoder.call(unpivotedData);

    if (updateInline) {
      file.writeAsStringSync(encodedResult);
    } else {
      stdout.write(encodedResult);
    }
  } on UsageException {
    outputHelpText();
  } on FormatException {
    outputHelpText();
  }
}

/// Given a table of [data], along with a set of column names to use as [keys], unpivots the data such that all table rows will be expanded to contain all key columns, plus the data for a single non-key column (thus expanding into as many new rows as there are non-key columns). The resulting rows are returned as an Iterable of Iterables, with no header information.
Iterable<Iterable<dynamic>> unpivotData(grizzly_io.Table data, List<String> keys) {
  return data.toMap().expand<Iterable<dynamic>>((Map<String, dynamic> rowMap) sync* {
    Map<String, dynamic> keyColumns = Map.from(rowMap)..removeWhere((key, value) => !keys.contains(key));
    Map<String, dynamic> nonKeyColumns = Map.from(rowMap)..removeWhere((key, value) => keys.contains(key));
    for (MapEntry<String, dynamic> entry in nonKeyColumns.entries) {
      yield keyColumns.values.followedBy([entry.key, entry.value]);
    }
  });
}

/// Given an [iterable] of Iterables, causes all nested Iterables, as well as the outer [iterable], to be fully iterated, by way of calling [Iterable.toList()] on them. The resulting List of Lists is returned.
List<List<T>> reifyNestedIterables<T>(Iterable<Iterable<T>> iterable) =>
    iterable.map((Iterable<T> subIterable) => subIterable.toList()).toList();
