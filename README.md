## What is this?

Stashpy aims to be a slimmed-down Python 3 replacement for
[Logstash](https://www.elastic.co/products/logstash), a log
aggregator. Stashpy accepts connections on a TCP port, parses messages
passed to it over this connection, and indexes them on an
ElasticSearch cluster.

Stashpy is still in development.


## Installing and running

Stashpy requires Python 3. All Linux distros have a relatively new
version in their official repositories. On Mac OS, the Homebrew
version is recommended.

Among the various methods of installing package dependencies,
virtualenv is recommended. Python 3.5 comes with pyvenv, which is a
built-in equivalent of virtualenv. For earlier versions, you will need
to manually install virtualenv with `sudo pip install virtualenv.  If
you already have a 2.* version of Python installed together with
virtualenv, you can install Python 3 and call virtualenv with the
`--python` argument to use the new version, as in `virtualenv stashpy
--python \`which python3\``.

It is possible to install Stashpy simply as a dependency or with `pip
install stashpy` as it is [on
PyPI](https://pypi.python.org/pypi/stashpy). For development purposes,
you can simply create a virtualenv wherever you prefer, and run
Stashpy from there. The entry point is named `stashpy`, and accepts a
configuration file as the only argument. A sample configuration file
`sample-config.yml` is provided. See the Usage section for
documentation on different configuration options.

The recommended way to run Stashpy as a service is by checking it out
into `/opt`, and also creating a virtualenv there. To run as an
upstart-managed service, consult the provided
`stashpy.conf`. Unfortunately, we haven't moved on to systemd yet, so
no sample configuration for that, but it shouldn't be too difficult.


## Usage

As can be seen from the extension of `sample-config.yml`, the
configuration format is YAML. Configuration options are the following:

* `address`, `port`: The address and port on which Stashpy should listen.

* `indexer_config`: Configuration options for the ElasticSearch
  cluster to index on. Must have the following keys:

  - `host`: ES host

  - `port`: ES port

  - `index_pattern`: The base pattern to be used for determining index
    name. This pattern will be passed on to
    [`datetime.strftime`](https://docs.python.org/3/library/datetime.html#datetime.date.strftime),
    and will then be formatted with the parsed values dictionary.

* `logging`: This option will be passed on as-is to the
  `logging.config.dictConfig` method. If it is not supplied,
  `stashpy.main.DEFAULT_LOGGING`, which simply logs to stdout, will be
  used.

* `processor_spec`: The parsing specification. See the next section
  for details.


## Parsing Specification

Stashpy turns log lines (i.e. strings that end with a newline)
supplied through a TCP connection into JSON documents, and indexes
these in ElasticSearch. The log lines are parsed using regular
expressions, which can be specified in one of two different formats:
the **parse format**, or **Oniguruma-flavored named regular
expressions**. The first of these is a normal Python 3 formattable
string that is processed using the [parse
library](https://pypi.python.org/pypi/parse). An example for such an
expression is the following:

* `My name is {name} and I'm {age:d} years old`

Parsing the sentence `My name is Afro and I'm 40 years old` would lead
to the JSON document `{"age": 40, "name": "Afro"}`.

The second format, Oniguruma-flavored regular expressions, uses the
[regex library](https://pypi.python.org/pypi/regex) to provide an
experience similar to that of the [grok plugin for
Logstash](https://www.elastic.co/guide/en/logstash/current/plugins-filters-grok.html). In
an Oniguruma RE, you can build complex regular expressions by
combining simpler components [??]. For example, take the following
regular expression:

* `My name is %{USERNAME:name} and I'm %{INT:age} years old`

This expression, when used as a specification, leads to the following
regular expression:

* `My name is (?P<name>[a-zA-Z0-9._-]+) and I'm (?P<age>(?:[+-]?(?:[0-9]+))) years old`

See the file `stashpy/patterns/grok_patterns.txt` for a list of the
various components you can use in your regular expressions.

### By `processor_spec`

The first method is by providing the `processor_spec` option in the
configuration file. This option can have two keys:

* `to_dict`: A list of expressions that are turned into JSON documents
  without further processing.

* `to_format`: A list of dictionaries whose keys are specifications
  and values are dictionariers that are to be formatted based on
  parsed values.

For both, the specification can be either in the format specification,
parsed using the
or a regular expression with named patterns. For example, the
following are equivalent ways of parsing the same pattern:


Either of these patterns, when included as an element of `to_dict`,
will parse the sentence `My name is Yuri and I'm 25 years old.`. The
first one will result in the following Python dictionary:

    {"name": "Yuri", "age": 25}

The second line, however, will not provide a conversion of the `age`
value into an integer, and will result in the following:

    {"name": "Yuri", "age": "25"}

If you want the parsed data to be subject to further modification and
conversion, have a look at class-based processing below.

The `to_format` option requires the formatting dictionary to be
specified as the value for the parsing specification key. In the
configuration, it would look like this:

    to_format:
      "Her name is {name} and she's {age:d} years old.":
        name_line: "Name is {name}"
        age_line: "Age is {age}"

As you can see, all values in this dictionary are interpolated as
strings.

#### A note about regular expressions

Stashpy uses the [regex package](https://pypi.python.org/pypi/regex),
instead of the built-in `re` module due to its compatibility with the
Onigiruma patterns used in
[Grok](https://www.elastic.co/guide/en/logstash/current/plugins-filters-grok.html). This
enables you to use the built-in regular expression shortcuts
[available in
Grok](https://github.com/elastic/logstash/blob/v1.4.2/patterns/grok-patterns). These
patterns are included in Stashpy, so you don't have to include
them. If you are surprised by the regular expression behavior, though,
refer to the regex documentation.

### Custom class

The second method of processing log lines is by specifying a class
that is responsible for accepting them and returning dictionaries. The
path of this class can be passed using the `processor_class`
option. This class must subclass `stashpy.LineProcessor` and implement
the method `for_line(self, line)`, which will be called for each log
line. Two useful methods from the parent class that can be used for
more specialized processing are `do_dict_specs(self, line)`, and
`do_format_specs`. The first method returns the result for the first
match from the `self.dict_specs` list, while the second does the same
for the `self.format_specs` attribute. If your class has the class
attributes `TO_DICT` or `TO_FORMAT`, these will be used to populate
the instance attributes. The following example is equivalent to what
happens in the default processing pipeline:


### Processing pipeline

The order of processing in Stashpy is relatively
straightforward. First, the `to_dict` specs are applied; if any of the
patterns match, the resulting dictionary is returned. If there are no
such matches, the `to_format` specs are applied, and the result from
the first match is returned. If you are using a custom class for
processing, you can introduce your own ordering and logic.

## Testing

One thing that really annoyed me about Logstash was that testing
patterns was incredibly difficult; the only reliable test could be
done on the live system. Stashpy aims to make testing patterns
simpler. In order to test a parsing specification, simply subclass
`stashpy.tests.PatternTest`. This class has three methods that can be
used to process a logline with a parsing specification:

* `process_to_dict(self, to_dict_spec, logline)`: Processes a list of
  to dictionary parsing specifications, and returns result.

* `process_to_format(self, to_format_spec, logline)`: Processes a list
  of to format parsing specifications, and returns result.

Here is a sample test:
```python
import unittest

from stashpy.tests import PatternTest

class SampleTest(PatternTest):

    def test_pattern(self):
        self.assertDictEqual(
            self.process_to_dict(["My name is {name} and I'm {age:d} years old."],
                                 "My name is Yuri and I'm 3 years old."),
            {'name': 'Yuri', 'age': 3})

if __name__ == '__main__':
    unittest.main()
```
