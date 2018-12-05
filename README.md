# SON_Frequent_Item_Set


### Son Algorithm details

* Repeatedly read small subsets of the baskets into main memory
* Run an in-memory algorithm (e.g., a priori, random sampling) to find all frequent itemsets(Note: we are not sampling, but processing the entire file in memory-sized chunks)
* An itemset becomes a candidate if it is found to be frequent in any one or more subsets of the baskets.
* On a second pass, count all the candidate itemsets and determine which are frequent in the entire set
* Key “monotonicity” idea: an itemset cannot be frequent in the entire set of baskets unless it is frequent in at least one subset


