Computes the near duplicate of the documents and emit pairs of documents having a similarity value greater than a chosen threshold. The algorithm used a prefix and length filter in order to avoid computing all possible pairs of documents in the collections. The length filter has the property that |P| ≥ t X|Q|(tє[0,1]) where |P| is the length of document P, and |Q| length of document Q with |Q| > |P|. The prefix filter has a property that two documents must have at least one common prefix if their similarity is not lesser than a given threshold. For document P, the prefix length is defined as prefixlength = |P| - [t x |P|] +1. 