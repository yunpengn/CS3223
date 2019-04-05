[![Build Status](https://travis-ci.com/yunpengn/CS3223.svg?token=pkVciw13vjdfspFfg5fy&branch=master)](https://travis-ci.com/yunpengn/CS3223)

# CS3223 Database Systems Implementation Project

**_AY2018/2019 Semester 2<br>
School of Computing<br>
National University of Singapore_**

This project focuses on the implementation of a simple _SPJ (Select-Project-Join)_ query engine to illustrate how query processing works in modern database management systems (DBMS), specifically relational databases (RDBMS). More information about the project requirements can be found at [here](https://www.comp.nus.edu.sg/~tankl/cs3223/project.html).

This repository presents our approach to this project. We are a team consisting of undergraduate students from the [National University of Singapore](http://www.nus.edu.sg), comprising of

- [Niu Yunpeng](https://github.com/yunpengn)
- [Sun Shengran](https://github.com/dalessr)

## Implementation Outline

Based on the given [template](http://www.comp.nus.edu.sg/~tankl/cs3223/project/Component.tar.gz), we have implemented the following operators in this SPJ query engine:
- Block Nested Loops Join (see [BlockNestedJoin.java](src/qp/operators/BlockNestedJoin.java))
- Sort Merge join (see [SortMergeJoin.java](src/qp/operators/SortMergeJoin.java))
- External sort (using k-way merge algorithm) (see [Sort.java](src/qp/operators/Sort.java))
- `DISTINCT` operator (see [Distinct.java](src/qp/operators/Distinct.java))
- `GROUP BY` operator (see [Groupby.java](src/qp/operators/Groupby.java))

We have tried to follow the [Volcano iterator model](https://db.in.tum.de/~grust/teaching/ws0607/MMDBMS/DBMS-CPU-5.pdf) to implement the various operators. However, there does exist some operators (such as `Sort`) which are blocking and cannot use the iterator model.

In addition, we have implemented a new hybrid randomized operator (see [here](https://github.com/yunpengn/CS3223/blob/master/src/QueryMain.java#L117)), which consists of both the iterative improvement (II) algorithm (see [RandomII.java](src/qp/optimizer/RandomII.java)) and the simulated annealing (SA) algorithm (see [RandomSA.java](src/qp/optimizer/RandomSA.java)).

## Setup Instructions

- Make sure you have installed [Java](https://www.java.com) not lower than JDK1.8.
	- Looks like the project works fine in a newer version of Java (like JDK 11) as well.
- Make sure you have installed the latest version of [IntelliJ IDEA](https://www.jetbrains.com/idea/).
- Clone the repository into your local repository by `git clone git@github.com:yunpengn/CS3223.git`.
- Make sure you have configured the import order in IDE correctly:
	- Go to `File` > `Settings...` (Windows/Linux), or `IntelliJ IDEA` > `Preferences...` (macOS);
	- Select `Editor` > `Code Style` > `Java` and choose the `Imports` tab;
	- Set both `Class count to use import with '*'` and `Names count to use static import with '*'` to 999;
	- For `Import Layout`: follow the order of `import static all other imports`, `<blank line>`, `import java.*`, `<blank line>`, `import javax.*`, `<blank line>`, `import org.*`, `<blank line>`, `import com.*`, `<blank line>`, `import all other imports`.
- Select `Import Project` from the `CS3223` folder you have just cloned to create the project:
	- Use `Create project from existing sources` option;
	- Go to `Project Structure` > `Libraries`, add `Java` with `lib/CUP`, `lib/JLEX`, `lib/hamcrest-core-1.3.jar` and `lib/junit-4.13-beta-2.jar`.
	- Go to `Project Structure` > `Modules`:
		- Mark `src` and `testcases` folder as `Sources`;
		- Mark `test` folder as `Tests`;
		- Mark `classes` folder and `out` folder (if exists) as `Excluded`.
- Run `build.bat` (Windows) or `build.sh` (macOS or Linux) to build the project.

## Development Workflow

- We follow the [feature branch workflow](https://www.atlassian.com/git/tutorials/comparing-workflows/feature-branch-workflow)
    - That means, you should NOT fork this repository. Instead, create a new branch and work on it.
- Every single line of code must be reviewed by someone else.
    - Whenever you create a PR, assign it to yourself and request review from someone else.
    - After a PR is merged, delete the branch immediately (using the button on the page of that PR so that it is possible to revert later, do NOT delete it manually).
- Follow the normal coding styles. You should especially pay attention to the following aspects:
    - Never use wildcard import. Only import the classes you need;
    - Every class & method should have a header comment using valid syntax of [JavaDoc](https://docs.oracle.com/javase/8/docs/technotes/tools/windows/javadoc.html);
    - Each indentation level should 4 spaces (rather than tab);
    - Use logging if necessary (for this project, we would use `System.out.println` & `System.err.println` for simplicity).
- To generate the scanner class with JLex library, follow the steps below:
    - Go to the `lib` folder by `cd lib/`;
    - Run `java JLex.Main ../src/qp/parser/scanner.lex`;
    - The command in last step will generate a new file named `scanner.lex.java`, use this file to replace the original `Scanner.java` file. Make sure you remember to change the class name inside the file as well (as the original generated file name is not desirable).
- To generate the parser class with the JavaCup library, follow the steps below:
    - Go to the `lib/CUP` folder by `cd lib/CUP/`;
    - Run `java java_cup.Main ../../src/qp/parser/parser.cup`;
    - Replace the original `parser.java` and `sym.java` with the generated files.

## References

- [CS3223 Module Website](https://www.comp.nus.edu.sg/~tankl/cs3223)
- [JLex: A Lexical Analyzer Generator for Java](http://www.cs.princeton.edu/~appel/modern/java/JLex/)
- [CUP: Parser Generator for Java](http://www.cs.princeton.edu/~appel/modern/java/CUP/)

## Licence

[GNU General Public Licence 3.0](LICENSE)
