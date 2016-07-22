class MyService : StatefulService
{
    private Task<IReliableDictionary<int, string>> AccountNames => StateManager.GetOrAddAsync<IReliableDictionary<int, string>>("AccountNames");
    private Task<IReliableDictionary<int, string>> AccountData => StateManager.GetOrAddAsync<IReliableDictionary<int, string>>("AccountData");

    public async Task<List<Account>> SearchAccountsByNameAsync(string name)
    {
        using (var txn = StateManager.CreateTransaction())
        {
            var accountNames = await AccountNames;
            var accountData = await AccountData;
            var accounts = await (await accountNames.CreateLinqAsyncEnumerable(txn))
                .Where(x => x.Value.IndexOf(name, StringComparison.InvariantCultureIgnoreCase) >= 0)
                .SelectAsync(async x => new Account
                {
                    Id = x.Key,
                    Name = x.Value,
                    Data = (await accountData.TryGetValueAsync(txn, x.Key)).Value
                })
                .ToList();
            return accounts;
        }
    }
}