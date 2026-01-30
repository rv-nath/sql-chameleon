#  SQL Chameleon
A command line tool for converting any bunch of sql (mostly DDL)
to a specified target dialect.  For example, convert from MySQL to Oracle or vice versa.

Philosopy:
- The program reads in an sql file of a given dialect from cmdline.
- Parses the content into a dialect agnostic AST.
- Emits the target sql statements.

Distant Roadmap:
- The program can have an API that serves up data to a web UI, that can render the tables and their links in a graphical manner.

- GUI may have options for editing, saving, conversion of schemas from and to any dialect.
