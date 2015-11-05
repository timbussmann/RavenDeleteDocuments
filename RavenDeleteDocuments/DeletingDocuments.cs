using System;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using NUnit.Framework;
using Raven.Abstractions.Exceptions;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;

namespace RavenDeleteDocuments
{
    [TestFixture]
    public class DeletingDocuments
    {
        [Test]
        public void ConcurrentDeletes()
        {
            var store = CreateStore();

            var document = new DemoDocument();
            using (var session = store.OpenSession())
            {
                session.Store(document);
                session.SaveChanges();
            }

            var documentLoaded = new CountdownEvent(2);
            var documentDeleted = new CountdownEvent(2);

            var t1 = Task.Run(() => DeleteDocument(store, document.Id, documentLoaded, documentDeleted));
            var t2 = Task.Run(() => DeleteDocument(store, document.Id, documentLoaded, documentDeleted));

            Assert.IsTrue(t1.Result | t2.Result, "the document should be deleted");
            Assert.IsFalse(t1.Result && t2.Result, "only one operation should complete successfully");
        }

        [Test]
        public void ConcurrentDeletesWithDtc()
        {
            var store = CreateStore();

            var document = new DemoDocument();
            using (var session = store.OpenSession())
            {
                session.Store(document);
                session.SaveChanges();
            }

            var documentLoaded = new CountdownEvent(2);
            var documentDeleted = new CountdownEvent(2);

            var t1 = Task.Run(() =>
            {
                using (var tx = new TransactionScope(TransactionScopeOption.RequiresNew))
                {
                    var result = DeleteDocument(store, document.Id, documentLoaded, documentDeleted);
                    tx.Complete();
                    return result;
                }
            });

            var t2 = Task.Run(() =>
            {
                using (var tx = new TransactionScope(TransactionScopeOption.RequiresNew))
                {
                    var result = DeleteDocument(store, document.Id, documentLoaded, documentDeleted);
                    tx.Complete();
                    return result;
                }
            });

            Assert.IsTrue(t1.Result | t2.Result, "the document should be deleted");
            Assert.IsFalse(t1.Result && t2.Result, "only one operation should complete successfully");
        }

        [Ignore("requires unstable raven package")]
        [Test]
        public async Task ConcurrentDeletesWithDtcAsync()
        {
            var store = CreateStore();

            var document = new DemoDocument();
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(document);
                await session.SaveChangesAsync();
            }

            var documentLoaded = new CountdownEvent(2);
            var documentDeleted = new CountdownEvent(2);

            var t1 = Task.Run(async () =>
            {
                using (var tx = new TransactionScope(TransactionScopeOption.RequiresNew, TransactionScopeAsyncFlowOption.Enabled))
                {
                    var result = await DeleteDocumentAsync(store, document.Id, documentLoaded, documentDeleted);
                    tx.Complete();
                    return result;
                }
            });

            var t2 = Task.Run(async () =>
            {
                using (var tx = new TransactionScope(TransactionScopeOption.RequiresNew, TransactionScopeAsyncFlowOption.Enabled))
                {
                    var result = await DeleteDocumentAsync(store, document.Id, documentLoaded, documentDeleted);
                    tx.Complete();
                    return result;
                }
            });

            Assert.IsTrue(t1.Result | t2.Result, "the document should be deleted");
            Assert.IsFalse(t1.Result && t2.Result, "only one operation should complete successfully");
        }

        private static bool DeleteDocument(IDocumentStore store, Guid documentId, CountdownEvent documentLoaded,
            CountdownEvent documentDeleted)
        {
            try
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;

                    var document = session.Load<DemoDocument>(documentId);

                    documentLoaded.Signal(1);
                    documentLoaded.Wait();

                    session.Delete(document);

                    documentDeleted.Signal(1);
                    documentDeleted.Wait();

                    session.SaveChanges();
                    return true;
                }
            }
            catch (ConcurrencyException e)
            {
                Console.WriteLine(e);
                return false;
            }
        }

        private static async Task<bool> DeleteDocumentAsync(IDocumentStore store, Guid documentId, CountdownEvent documentLoaded,
            CountdownEvent documentDeleted)
        {
            try
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;

                    var document = await session.LoadAsync<DemoDocument>(documentId);

                    documentLoaded.Signal(1);
                    documentLoaded.Wait();

                    session.Delete(document);

                    documentDeleted.Signal(1);
                    documentDeleted.Wait();

                    await session.SaveChangesAsync();
                    return true;
                }
            }
            catch (ConcurrencyException e)
            {
                Console.WriteLine(e);
                return false;
            }
        }

        class DemoDocument
        {
            public Guid Id { get; set; } 
        }

        private static IDocumentStore CreateStore()
        {
//            return new DocumentStore
//            {
//                Url = "http://localhost:8083"
//            }.Initialize();

            return new EmbeddableDocumentStore
            {
                RunInMemory = false
            }.Initialize();
        }
    }
}