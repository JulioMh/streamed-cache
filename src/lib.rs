use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use std::{
    collections::HashMap,
    result::Result,
    sync::{Arc, Mutex},
};

type City = String;
type Temperature = u64;

#[async_trait]
pub trait Api: Send + Sync + 'static {
    async fn fetch(&self) -> Result<HashMap<City, Temperature>, String>;
    async fn subscribe(&self) -> BoxStream<Result<(City, Temperature), String>>;
}

pub struct StreamCache {
    results: Arc<Mutex<HashMap<String, u64>>>,
}

impl StreamCache {
    pub fn new(api: impl Api) -> Self {
        let instance = Self {
            results: Arc::new(Mutex::new(HashMap::new())),
        };
        instance.update_in_background(api);
        instance
    }

    pub fn get(&self, key: &str) -> Option<u64> {
        let results = self.results.lock().expect("poisoned");
        results.get(key).copied()
    }

    pub fn update_in_background(&self, api: impl Api) {
        let api_arc = Arc::new(api);

        tokio::spawn({
            let api_fetch = Arc::clone(&api_arc);
            let results_fetch = Arc::clone(&self.results);
            async move {
                StreamCache::manage_fetch(results_fetch, api_fetch).await;
            }
        });

        tokio::spawn({
            let api_subscribe = Arc::clone(&api_arc);
            let results_subscribe = Arc::clone(&self.results);
            async move {
                StreamCache::manage_subscribe(results_subscribe, api_subscribe).await;
            }
        });
    }

    async fn manage_fetch(cache: Arc<Mutex<HashMap<String, u64>>>, api: Arc<impl Api>) {
        let data: Result<HashMap<String, u64>, String> = api.fetch().await;

        match data {
            Ok(cities) => cities.into_iter().for_each(|(city, temperature)| {
                println!(
                    "New data: City = {:?}, Temperature = {:?}",
                    city, temperature
                );
                let mut lock = cache.lock().unwrap();
                match lock.get(&city) {
                    None => {
                        lock.insert(city, temperature);
                    }
                    Some(_) => (),
                }
            }),
            Err(err) => eprintln!("Error fetching data: {:?}", err),
        }
    }

    async fn manage_subscribe(cache: Arc<Mutex<HashMap<String, u64>>>, api: Arc<impl Api>) {
        let subscriber = api.subscribe().await;
        let messages = subscriber.into_stream();

        tokio::pin!(messages);

        while let Some(msg) = messages.next().await {
            match msg {
                Ok((city, temperature)) => {
                    println!(
                        "Update received: City = {:?}, Temperature = {:?}",
                        city, temperature
                    );
                    cache.lock().unwrap().insert(city, temperature);
                }
                Err(err) => eprintln!("Error in subscription stream: {:?}", err),
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio::time;

    use futures::{future, stream::select, FutureExt, StreamExt};
    use maplit::hashmap;

    use super::*;

    #[derive(Default)]
    struct TestApi {
        signal: Arc<Notify>,
    }

    #[async_trait]
    impl Api for TestApi {
        async fn fetch(&self) -> Result<HashMap<City, Temperature>, String> {
            // fetch is slow an may get delayed until after we receive the first updates
            self.signal.notified().await;
            Ok(hashmap! {
                "Berlin".to_string() => 29,
                "Paris".to_string() => 31,
            })
        }
        async fn subscribe(&self) -> BoxStream<Result<(City, Temperature), String>> {
            let results = vec![
                Ok(("London".to_string(), 27)),
                Ok(("Paris".to_string(), 32)),
            ];
            select(
                futures::stream::iter(results),
                async {
                    self.signal.notify_one();
                    future::pending().await
                }
                .into_stream(),
            )
            .boxed()
        }
    }
    #[tokio::test]
    async fn works() {
        let cache = StreamCache::new(TestApi::default());

        // Allow cache to update
        time::sleep(Duration::from_millis(100)).await;

        assert_eq!(cache.get("Berlin"), Some(29));
        assert_eq!(cache.get("London"), Some(27));
        assert_eq!(cache.get("Paris"), Some(32));
    }
}
