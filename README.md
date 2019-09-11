# ddb

The library implements collection of Erlang drivers to AWS storage services that operates with generic representation of algebraic data types. 


## Inspiration

The library encourages developers to use Erlang records to define domain models, write correct, maintainable code. The library uses [generic programming style](https://en.wikipedia.org/wiki/Generic_programming) to implement actual storage I/O, while expose external domain object in terms of types to-be-specified-later with implicit conversion back and forth between a concrete records (ADT).

Essentially, it uses generics to implement a following key-value trait to access domain objects. 

```scala
trait KeyVal[T] {
  def put(entity: T): T
  def get(pattern: T): T
  def remove(pattern: T): T
  def update(entity: T): T
  def match(pattern: T): Seq[T]
}
```

The library support
- [x] [AWS DynamoDB](https://aws.amazon.com/dynamodb/)
- [ ] [AWS S3](https://aws.amazon.com/s3/)


## Getting started

The latest version of the library is available at its `master` branch. All development, including new features and bug fixes, take place on the `master` branch using forking and pull requests as described in contribution guidelines.

```erlang
{deps, [
  {ddb, ".*",
    {git, "https://github.com/fogfish/ddb", {branch, master}}
  }
]}.
```

Define an application domain model using product types, which are strongly expressed by records in Erlang.

```erlang
-type fullname() :: binary().
-type address()  :: binary().
-type city()     :: binary().

-record(person, {
  id      :: binary(),
  name    :: fullname(),
  address :: address(), 
  city    :: city()
}).
```

Use semi-automated partial application to make a generic processing for your data domain and spawn an implicit I/O endpoint for ADT. 
 
```erlang
ddb:start_link(
  #person{},                    %% an empty ADT defines a type
  "ddb://dynamodb.eu-west-1.amazonaws.com:443/test",  %% AWS service URI endpoint
  labelled:encode(#person{}),   %% a labelled generic encoder of ADT
  labelled:decode(#person{})    %% a labelled generic decoder of ADT
).
```

Creates a new entity, or replaces an old entity with a new value.

```erlang
ddb:put(
  #person{
    id      = <<"deadbeef">>,
    name    = "Verner Pleishner",
    age     = 64,
    address = "Blumenstrasse 14, Berne, 3013"
  }
)
```

Lookup the entity

```erlang
ddb:get(#person{id = <<"deadbeef">>}).
```

Remove the entity

```erlang
ddb:remove(#person{id = <<"deadbeef">>}).
```

Partial update of the entity

```erlang
ddb:update(#person{id = <<"deadbeef">>, age = 65}).
```

## How To Contribute

The library is [Apache 2.0](LICENSE) licensed and accepts contributions via GitHub pull requests:

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

The development requires [Erlang/OTP](http://www.erlang.org/downloads) version 19.0 or later and essential build tools.

**Build** and **run** service in your development console. The following command boots Erlang virtual machine and opens Erlang shell.

```bash
git clone https://github.com/fogfish/ddb
cd ddb
make
make run
```

### commit message

The commit message helps us to write a good release note, speed-up review process. The message should address two question what changed and why. The project follows the template defined by chapter [Contributing to a Project](http://git-scm.com/book/ch5-2.html) of Git book.

>
> Short (50 chars or less) summary of changes
>
> More detailed explanatory text, if necessary. Wrap it to about 72 characters or so. In some contexts, the first line is treated as the subject of an email and the rest of the text as the body. The blank line separating the summary from the body is critical (unless you omit the body entirely); tools like rebase can get confused if you run the two together.
> 
> Further paragraphs come after blank lines.
> 
> Bullet points are okay, too
> 
> Typically a hyphen or asterisk is used for the bullet, preceded by a single space, with blank lines in between, but conventions vary here
>
>

### bugs

If you experience any issues with the library, please let us know via [GitHub issues](https://github.com/fogfish/datum/issue). We appreciate detailed and accurate reports that help us to identity and replicate the issue. 

* **Specify** the configuration of your environment. Include which operating system you use and the versions of runtime environments. 

* **Attach** logs, screenshots and exceptions, in possible.

* **Reveal** the steps you took to reproduce the problem, include code snippet or links to your project.


## Licensee

[![See LICENSE](https://img.shields.io/github/license/fogfish/ddb.svg?style=for-the-badge)](LICENSE)

