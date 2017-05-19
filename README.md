# BigDataAlgorithmLab1
# Andrei Constantinecu

# 4.1 Tf-IDF

To realise our TF-IDF implementation in hadoop, 5 map-reduce passes were implemented. About text processsing, for simplicity reasons, we deleted all characters that were not alpha-numerical, and we set everything to lowercase. Here is in detail what each of these passes do.

1- Inverted index : This pass counts in how many documents each word has occured. Since we only wahe two document, the possible values are either 1 or 2.

2- Total word count count per document : this passes serves to count the total number of words per document.

3- First tf-idf pass : this pass serves to process the data in order to put it in the right format. According to the instructions given in the pdf this pass serves the purpose of counting the occurence of each word in each document.

4 - Second tf-idf pass : This time we process the data so as to include the total number of words of the document of interest, including the information from 2.

5- Third tf-idf pass : Here we include the information from 1, and thus we have all the needed information to compute the tf-idf scores of each word. Below is the list of the 20 words with the highest tf-idf score. More on the results can be found in the results folder. All the code, and the results can be found in the appropriate folder of the repository.


| Word        | Doc           | Score  |
| ------------- |:-------------:| -----:|
| buck      | callwild.txt | 0.006827639 |
| dogs      | callwild.txt      |   0.002443117 |
| thornton | callwild.txt      |    0.001766897 |
| myself      | defoe-robinson.txt | 0.001668513 |
| spitz      | callwild.txt      |   0.001308813 |
| sled | callwild.txt      |    0.001308813 |
| francois     | callwild.txt | 0.001134304 |
| bucks     | callwild.txt      |   0.001025237 |
| friday | defoe-robinson.txt     |    0.001022821 |
| trail      | callwild.txt | 0.000894355 |
| john      | callwild.txt      |   0.000872542 |
| perrault | callwild.txt      |    0.000807101 |
| team      | callwild.txt | 0.000654406 |
| hal      | callwild.txt      |   0.000654406 |
| thoughts | defoe-robinson.txt      |    0.000634264 |
| traces      | callwild.txt      |   0.000610779 |
| ice | callwild.txt      |    0.000610779 |
| solleks      | callwild.txt | 0.000588966 |
| around      | callwild.txt      |   0.000567152 |
| dave | callwild.txt      |    0.000523525 |


# 4.2 Page-Rank

For this exercise we implemented the page-rank algorithm in a rather unconventional way, with 3 map-reduce passes, not using the usual framework of matrix vector multiplication.

1 - First pass : we process the data so as to have for each page its score (1 if initialization, else read the last file with the scores), the nodes to which it points, and the total number of such out going links.

2- Second pass : here we can already compute an update page rank score. The idea is that fort the mapper, we use each outgoing link as key, and as value we put the current link, its current page rank score, as well as its total number of outgoing links. With this information, it this possible in the reducer by going through all values to compute the updated page rank score.

3- Third pass : this pass serves only the purposes to verify the gap between the last iteration and the current one, to see if the difference is sufficiently small as to end the iteration process.

It was also necessary to write a bash script in order to automatize the iteration process. The page rank converged after 70 iterations.

Note: We used here the unscaled version of the page-rank algorithm, not dividing the score by N (the total number of nodes). We encounter a problem since when we sum all our page-rank scores we do not obtain N. We failed to provided an explanation of why we encounter this problem.

All the code, including the bash script, and the results can be found in the appropriate folder of the repository.
