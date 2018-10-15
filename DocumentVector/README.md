These method creates document vectors of all the tokens in the dictionary. In order to create these vectors, the document frequency ordering dictionary is used. A hash table is initialized to store the document frequency ordering dictionary. The hash table is of  structure <key, value> , where key is the word and value is the word location in the document frequency dictionary ordering. The document vector is of the format filename@docLength@vectortext. Given document P, the document vector is P@6@2,3,5,8,9,10 and for Q is Q@8@1,3,4,5,6,7,9,10.

[forecast,1]  [paul,2]  [perterburg,3]  [rock,4] [saint,5][umbrella,6] [weather,7][prepare,8][rain,9] [today,10]

