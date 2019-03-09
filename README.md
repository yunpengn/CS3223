# CS3223 Database Systems Implementation Project

**_AY2018/2019 Semester 2<br>
School of Computing<br>
National University of Singapore_**

This project focuses on the implementation of a simple _SPJ (Select-Project-Join)_ query engine to illustrate how query processing works in modern database management systems (DBMS), specifically relational databases (RDBMS). More information about the project requirements can be found at [here](https://www.comp.nus.edu.sg/~tankl/cs3223/project.html).

This repository presents our approach to this project. We are a team of 3 undergraduate students from the [National University of Singapore](http://www.nus.edu.sg), comprising of

- [Niu Yunpeng](https://github.com/yunpengn)
- [Sun Shengran](https://github.com/dalessr)
- [Jia Zhixin](https://github.com/nusjzx)

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
	- Go to `Project Structure` > `Libraries`, add `Java` with `lib/CUP` and `lib/JLEX`.
	- Go to `Project Structure` > `Modules`:
		- Mark `src` folder as `Sources`;
		- Mark `classes` folder and `out` folder (if exists) as `Excluded`;
		- Mark `testcases` folder as `Resources`.
- Run `build.bat` (Windows) or `build.sh` (macOS or Linux) to build the project.

## References

- [CS3223 Module Website](https://www.comp.nus.edu.sg/~tankl/cs3223)
- [JLex: A Lexical Analyzer Generator for Java](http://www.cs.princeton.edu/~appel/modern/java/JLex/)
- [CUP: Parser Generator for Java](http://www.cs.princeton.edu/~appel/modern/java/CUP/)

## Licence

[GNU General Public Licence 3.0](LICENSE)
