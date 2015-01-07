"""
TODO implement and integrate
Entity inverted index for asynchronous connections
"""



class AsynchronousEntityInvertedIndex(InvertedIndex)
    def getEntitiesAsync(self, entityIds, callback):
        """Read entities asynchronously"""
        internalCallback = functools.partial(self.__getEntitiesAsyncCallback, entityIds, callback)
        keys = (EntityInvertedIndex._entityIdsToKey(k) for k in entityIds)
        self.conn.read(self.entityTableNo, keys, callback=internalCallback, mapKeys=mapKeys)
    def __getEntitiesAsyncCallback(self, entityIds, callback, values):
        entities = (self.unpackValue(val) for val in values)
        callback(zip(entityIds, minEntities))
    def __execAsyncSearch(self, searchFunc, callback, tokenObj, levels, limit=None):
        """
        Internal search runner for async multi-level search
        """
        allResults = []
        #Scan all levels
        states = [None] * len(levels) #Saves the result data for each level
        internalCallbackTpl = functools.partial(self.__execAsyncSearchScanCB, states, callback)
        #Start parallel jobs for each level
        for i, level in enumerate(levels):
            internalCallback = functools.partial(internalCallbackTpl, i)
            if limit is None:
                allResults = searchFunc(tokenObj, levels=levels, callback=internalCallback)
            else:
                allResults = searchFunc(tokenObj, levels=levels, limit=limit, callback=internalCallback)
    def __execAsyncSearchScanCB(self, states, callback, i, resultList):
        """Called from the async search runner once a scan result comes in"""
        states[i] = resultList
        if not any([state is None for state in states]): #All requests finished
            allResults = []
            #Append level results until minimal results are reacheds
            for result in states:
                allResults += result
                if len(allResults) >= self.minEntities: break
            #Remove duplicate results
            allResults = makeUnique(allResults)
            #Clamp to the maximum number of entities
            allResults = allResults[:self.maxEntities]
            #Read the entity objects
            self.getEntitiesAsync(allResults, callback=callback, mapKeys=True)
    def searchSingleTokenPrefixAsync(self, token, callback, levels=[""], limit=25):
        """
        Search a single token in the inverted index using async connection
        """
        assert self.connectionIsAsync
        return self.__execAsyncSearch(self.searchSingleTokenPrefixAsync, callback, token, levels, limit)
    def searchMultiTokenExactAsync(self, tokens, callback, levels=[""]):
        """Search multiple tokens in the inverted index, for exact matches"""
        assert self.connectionIsAsync
        return self.__execAsyncSearch(self.searchMultiTokenExactAsync, callback, tokens, levels)
    def searchMultiTokenPrefixAsync(self, tokens, callback, levels=[""], limit=25):
        """Search multiple tokens in the inverted index, for exact matches"""
        assert self.connectionIsAsync
        return self.__execAsyncSearch(
            super(EntityInvertedIndex, self).searchMultiTokenPrefixAsync, callback, tokens, levels, limit)
 