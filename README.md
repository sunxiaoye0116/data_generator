# data_generator
This project includes

1. a 1 to X-gram corpus converter NGramDataGenerator.java
edu.rice.bold.spark_data_generator.NGramDataGenerator <input_corpus_dir> <n-gram> <num_docs>
The new corpus will be placed in directory of ./<input_corpus_dir>_<n-gram>_gram
<num_docs> is 16923 for the default <input_corpus_dir> i.e. ./input/

2. a data generator DataGenerator.java that converts the input corpus to the input to applications
edu.rice.bold.spark_data_generator.DataGenerator <app_name> <num_tasks> <num_docs_per_task> <topic-k> <input_corpus_dir> <output_table_dir> <framework_name>
<app_name> lda
<num_tasks> 64 = 2*2*16
<num_docs_per_task> 264 = 16923/64
<topic-k> 100
<input_corpus_dir> ./input/
<output_table_dir> ./output_1_gram
<framework_name> spark
