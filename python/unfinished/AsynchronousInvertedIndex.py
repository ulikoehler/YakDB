"""
TODO implement and integrate
Inverted index for asynchronous connections
"""

class AsynchronousInvertedIndex(InvertedIndex):
    """
    Inverted index subclass that also provides asychronous methods
    TODO Implementation of most of these methods is unfinished
    """
    def searchMultiTokenPrefixAsync(self, tokens, callback, levels=[""], limit=25):
        """Search multiple tokens in the inverted index, for exact matches"""
        assert self.connectionIsAsync
        readKeys = [InvertedIndex.getKey(token, levels) for token in tokens]
        #In the results array, we set each index to the corresponding result, if any yet
        results = [None] * len(tokens)
        for i, token in enumerate(tokens):
            internalCallback = functools.partial(InvertedIndex.__searchMultiTokenPrefixAsyncRecvCallback,
                                                 callback, levels, results, i)
            #Here, we need access to both keys and values --> map the data into a dict
            self.searchSingleTokenPrefixAsync(token=token, levels=levels, limit=limit, callback=internalCallback)
    @staticmethod
    def __searchMultiTokenPrefixAsyncRecvCallback(origCallback, level, results, i, response):
        """This is called when the response for a multi token search has been received"""
        #Process & Call the callback if ALL requests have been finished
        results[i] = response
        if all([obj is not None for obj in results]):
            #The multi-token search algorithm uses only the level from the key.
            #Therefore, for read() emulation, we don't need to know the token
            result = None#TODO: InvertedIndex._processMultiTokenPrefixResult(results)
            origCallback(result)
    def searchMultiTokenExactAsync(self, tokens, callback, level=""):
        """Search multiple tokens in the inverted index, for exact matches"""
        assert self.connectionIsAsync
        readKeys = [InvertedIndex.getKey(token, level) for token in tokens]
        internalCallback = functools.partial(InvertedIndex.__searchMultiTokenExactAsyncRecvCallback, callback, level)
        #Here, we need access to both keys and values --> map the data into a dict
        self.conn.read(self.tableNo, readKeys, callback=internalCallback, mapKeys=True)
    @staticmethod
    def __searchMultiTokenExactAsyncRecvCallback(origCallback, level, response):
        """This is called when the response for a multi token search has been received"""
        result = None#TODO: InvertedIndex._processMultiTokenResult(response, level)
        origCallback(result)
    def searchSingleTokenPrefixAsync(self, token, callback, levels=[""], limit=25):
        """
        Search a single token in the inverted index using async connection
        """
        assert self.connectionIsAsync
        startKey = InvertedIndex.getKey(token, level)
        endKey = YakDBUtils.incrementKey(startKey)
        internalCallback = functools.partial(InvertedIndex.__searchSingleTokenPrefixAsyncRecvCallback, callback)
        self.conn.scan(self.tableNo, callback=internalCallback, startKey=startKey, endKey=endKey, limit=limit)
    @staticmethod
    def __searchSingleTokenPrefixAsyncRecvCallback(origCallback, response):
        "This gets called once an asynchronous request to "
        origCallback(InvertedIndex._processReadResult(response))
    def searchSingleTokenExactAsync(self, token, callback, levels=[""], limit=25):
        """
        Search a single token in the inverted index using async connection
        """
        assert self.connectionIsAsync
        readKeys = [InvertedIndex.getKey(token, level) for level in levels]
        internalCallback = functools.partial(
            InvertedIndex.__searchSingleTokenExactAsyncRecvCallback, callback)
        readResult = self.conn.read(self.tableNo,
            (InvertedIndex.getKey(token, level) for level in levels),
            callback = )
    @staticmethod
    def __searchSingleTokenExactAsyncRecvCallback(origCallback, readResult):
        origCallback(self._processReadResult(zip(levels, readResult)))
