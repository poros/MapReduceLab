-- Load input data from local input directory
A = LOAD './local-input/WORD_COUNT/sample.txt';
explain -out './explain/' -dot A
explain -out './explain/' A

-- Parse and clean input data
B = FOREACH A GENERATE FLATTEN(TOKENIZE((chararray)$0)) AS word;
explain -out './explain/' -dot B
explain -out './explain/' B

C = FILTER B BY word MATCHES '\\w+';
explain -out './explain/' -dot C
explain -out './explain/' C

-- Explicit the GROUP-BY / SHUFFLE Phase
D = GROUP C BY word;
explain -out './explain/' -dot D
explain -out './explain/' D

-- Generate output data in the form: <word, counts>
E = FOREACH D GENERATE group, COUNT(C);
explain -out './explain/' -dot E
explain -out './explain/' E

-- Store output data in local output directory
store E into './local-output/WORD_COUNT/';
explain -out './explain/' -dot E
explain -out './explain/' E
