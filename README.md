## Stanford CoreNLP wrapper for Apache Spark

This package wraps [Stanford CoreNLP](http://stanfordnlp.github.io/CoreNLP/) annotators as Spark
DataFrame functions following the [simple APIs](http://stanfordnlp.github.io/CoreNLP/simple.html)
introduced in Stanford CoreNLP 3.7.0.

This package requires Java 8 and CoreNLP to run.
Users must include CoreNLP model jars as dependencies to use language models.

All functions are defined under `com.databricks.spark.corenlp.functions`.

* *`cleanxml`*: Cleans XML tags in a document and returns the cleaned document.
* *`tokenize`*: Tokenizes a sentence into words.
* *`ssplit`*: Splits a document into sentences.
* *`pos`*: Generates the part of speech tags of the sentence.
* *`lemma`*: Generates the word lemmas of the sentence.
* *`ner`*: Generates the named entity tags of the sentence.
* *`depparse`*: Generates the semantic dependencies of the sentence and returns a flattened list of
  `(source, sourceIndex, relation, target, targetIndex, weight)` relation tuples.
* *`coref`*: Generates the coref chains in the document and returns a list of
  `(rep, mentions)` chain tuples, where `mentions` are in the format of
  `(sentNum, startIndex, mention)`.
* *`natlog`*: Generates the Natural Logic notion of polarity for each token in a sentence, returned
  as `up`, `down`, or `flat`.
* *`openie`*: Generates a list of Open IE triples as flat `(subject, relation, target, confidence)`
  tuples.
* *`sentiment`*: Measures the sentiment of an input sentence on a scale of 0 (strong negative) to 4
  (strong positive).  

Users can chain the functions to create pipeline, for example:

~~~scala
import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._

val input = Seq(
  (1, "<xml>Stanford University is located in California. It is a great university.</xml>")
).toDF("id", "text")

val output = input
  .select(cleanxml('text).as('doc))
  .select(explode(ssplit('doc)).as('sen))
  .select('sen, tokenize('sen).as('words), ner('sen).as('nerTags), sentiment('sen).as('sentiment))

output.show(truncate = false)
~~~

~~~
+----------------------------------------------+------------------------------------------------------+--------------------------------------------------+---------+
|sen                                           |words                                                 |nerTags                                           |sentiment|
+----------------------------------------------+------------------------------------------------------+--------------------------------------------------+---------+
|Stanford University is located in California .|[Stanford, University, is, located, in, California, .]|[ORGANIZATION, ORGANIZATION, O, O, O, LOCATION, O]|1        |
|It is a great university .                    |[It, is, a, great, university, .]                     |[O, O, O, O, O, O]                                |4        |
+----------------------------------------------+------------------------------------------------------+--------------------------------------------------+---------+
~~~

### Dependencies

Because CoreNLP depends on `protobuf-java` 3.x but Spark 2.3 depends on `protobuf-java` 2.x,
we release `spark-corenlp` as an assembly jar that includes CoreNLP as well as its transitive dependencies,
except `protobuf-java` being shaded.
This might cause issues if you have CoreNLP or its dependencies on the classpath.

To use `spark-corenlp`, you need one of the CoreNLP language models:

~~~bash
# Download one of the language models. 
wget http://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/3.9.1/stanford-corenlp-3.9.1-models.jar
# Run spark-shell 
spark-shell --packages databricks/spark-corenlp:0.3.1-s_2.11 --jars stanford-corenlp-3.9.1-models.jar
~~~

### Acknowledgements

Many thanks to Jason Bolton from the Stanford NLP Group for API discussions.
