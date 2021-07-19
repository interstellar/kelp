package plugins

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stellar/kelp/api"
	"github.com/stellar/kelp/model"
	"github.com/stretchr/testify/assert"
)

var emptyAPIKeyBinance = api.ExchangeAPIKey{}

func Test_createStateEvents(t *testing.T) {

	events := createStateEvents()

	assert.NotNil(t, events)
}

func Test_binanceExchangeWs_GetTickerPrice(t *testing.T) {

	if testing.Short() {
		return
	}

	pair := model.TradingPair{Base: model.XLM, Quote: model.BTC}
	pairs := []model.TradingPair{pair}

	testBinanceExchangeWs, err := makeBinanceWs(emptyAPIKeyBinance)

	if !assert.NoError(t, err) {
		return
	}

	m, e := testBinanceExchangeWs.GetTickerPrice(pairs)
	if !assert.NoError(t, e) {
		return
	}

	assert.Equal(t, 1, len(m))

	ticker := m[pair]
	fmt.Printf("ticker price: bid=%s, ask=%s, last=%s\n", ticker.BidPrice.AsString(), ticker.AskPrice.AsString(), ticker.LastPrice.AsString())

	if !assert.True(t, ticker.AskPrice.AsFloat() < 1, ticker.AskPrice.AsString()) {
		return
	}
	if !assert.True(t, ticker.BidPrice.AsFloat() < 1, ticker.BidPrice.AsString()) {
		return
	}
	if !assert.True(t, ticker.BidPrice.AsFloat() < ticker.AskPrice.AsFloat(), fmt.Sprintf("bid price (%s) should be less than ask price (%s)", ticker.BidPrice.AsString(), ticker.AskPrice.AsString())) {
		return
	}
	if !assert.True(t, ticker.LastPrice.AsFloat() < 1, ticker.LastPrice.AsString()) {
		return
	}
}

func Test_binanceExchangeWs_GetOrderBook(t *testing.T) {

	if testing.Short() {
		return
	}

	testBinanceExchangeWs, e := makeBinanceWs(emptyAPIKeyBinance)
	if !assert.NoError(t, e) {
		return
	}

	for _, obDepth := range []int32{1, 5, 8, 10, 15, 16, 20} {

		pair := model.TradingPair{Base: model.XLM, Quote: model.BTC}
		ob, e := testBinanceExchangeWs.GetOrderBook(&pair, obDepth)
		if !assert.NoError(t, e) {
			return
		}
		assert.Equal(t, ob.Pair(), &pair)

		if !assert.True(t, len(ob.Asks()) > 0, len(ob.Asks())) {
			return
		}
		if !assert.True(t, len(ob.Bids()) > 0, len(ob.Bids())) {
			return
		}

		if !assert.True(t, len(ob.Asks()) <= int(obDepth), fmt.Sprintf("asks should be <= %d", obDepth)) {
			return
		}
		if !assert.True(t, len(ob.Bids()) <= int(obDepth), fmt.Sprintf("bids should be <= %d", obDepth)) {
			return
		}

		assert.True(t, ob.Asks()[0].OrderAction.IsSell())
		assert.True(t, ob.Asks()[0].OrderType.IsLimit())
		assert.True(t, ob.Bids()[0].OrderAction.IsBuy())
		assert.True(t, ob.Bids()[0].OrderType.IsLimit())
		assert.True(t, ob.Asks()[0].Price.AsFloat() > 0)
		assert.True(t, ob.Asks()[0].Volume.AsFloat() > 0)
		assert.True(t, ob.Bids()[0].Price.AsFloat() > 0)
		assert.True(t, ob.Bids()[0].Volume.AsFloat() > 0)
	}
}

func Test_binanceExchangeWs_GetLatestTradeCursor(t *testing.T) {

	if testing.Short() {
		return
	}

	startIntervalSecs := time.Now().Unix()

	testBinanceExchangeWs, err := makeBinanceWs(emptyAPIKeyBinance)

	if !assert.NoError(t, err) {
		return
	}

	cursor, e := testBinanceExchangeWs.GetLatestTradeCursor()
	if !assert.NoError(t, e) {
		return
	}
	endIntervalSecs := time.Now().Unix()

	if !assert.IsType(t, "string", cursor) {
		return
	}

	cursorString := cursor.(string)
	cursorInt, e := strconv.ParseInt(cursorString, 10, 64)
	if !assert.NoError(t, e) {
		return
	}

	if !assert.True(t, startIntervalSecs <= cursorInt, fmt.Sprintf("returned cursor (%d) should be gte the start time of the function call in millis (%d)", cursorInt, startIntervalSecs)) {
		return
	}
	if !assert.True(t, endIntervalSecs >= cursorInt, fmt.Sprintf("returned cursor (%d) should be lte the end time of the function call in millis (%d)", cursorInt, endIntervalSecs)) {
		return
	}
}

func Test_binanceExchangeWs_GetTradeHistory(t *testing.T) {

	if testing.Short() {
		return
	}

	testBinanceExchangeWs, err := makeBinanceWs(emptyAPIKeyBinance)

	if !assert.NoError(t, err) {
		return
	}

	pair := model.TradingPair{Base: model.XLM, Quote: model.BTC}
	tradeHistoryResult, e := testBinanceExchangeWs.GetTradeHistory(pair, nil, nil)
	if !assert.NoError(t, e) {
		return
	}

	if !assert.True(t, len(tradeHistoryResult.Trades) >= 0) {
		return
	}

	if !assert.NotNil(t, tradeHistoryResult.Cursor) {
		return
	}

	assert.Fail(t, "force fail")
}
