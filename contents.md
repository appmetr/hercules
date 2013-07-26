# Hercules – Flexible Apache Cassandra ORM for Java


## Introduction <a id="introduction"></a>

Hercules is a lightweight yet powerful ORM with an approach to provide OOP way of
interacting with Cassandra-specific data model.


### Features <a id="features"></a>

- plain entities (entity per row) and wide entities (entity by column in a row)
- flexible entity serialisation customisation
- managed entity indexes
- works great with DI frameworks
- lifecycle method/event support
- does not store null fields


### Get It! <a id="get-it"></a>

Current version is 0.1 &mdash; initial public release. Has been used only in several
internal projects. *Use it in production at your own risk.*

Maven repo (currently hosted on github – add this to `<repositories>` in your `pom.xml`):

	<repository>
		<id>hercules</id>
		<url>https://raw.github.com/appmetr/hercules/mvn-repo/</url>
	</repository>


Maven dependency (add this to `<dependencies>` in your `pom.xml`):

	<dependency>
		<groupId>com.appmetr</groupId>
		<artifactId>hercules</artifactId>
		<version>0.1</version>
	</dependency>



Latest binaries: [hercules-0.1.jar](https://raw.github.com/appmetr/hercules/mvn-repo/com/appmetr/hercules/0.1/hercules-0.1.jar)

Latest source code: [hercules-0.1-sources.jar](https://raw.github.com/appmetr/hercules/mvn-repo/com/appmetr/hercules/0.1/hercules-0.1-sources.jar)

Fork on GitHub: [https://github.com/appmetr/hercules](https://github.com/appmetr/hercules)

	


### Quick Start <a id="quick-start"></a>

Hercules requires Java SE 5 or higher.

Create you first entity:

	@Entity
	public class Cat {
		
		@Id String id;
		
		String name;
		
	}

	
Create entity DAO:

	public class CatDAO extends AbstractDAO<Cat, String> {

	    public CatDAO(Hercules hercules) {
	        super(Cat.class, hercules);
	    }

	}
	

Create Hercules config:

	Set<Class> entityClasses = new HashSet<Class>();
	entityClasses.add(Cat.class);

	HerculesConfig config = new HerculesConfig(
	    "Test", 			// keyspace name
	    "localhost:9160",	// cassandra host and port
	    1,					// replication factor
	    true,				// is schema modification enabled
	    entityClasses
	);


Create Hercules and perform queries:

	Hercules hercules = HerculesFactory.create(config);
	hercules.init();
	
	CatDAO catDAO = new CatDAO(hercules);
	Cat cat = catDAO.get("cat01");
	
	hercules.shutdown();
	


## Reference

Coming soon...


## Team

Coming soon...


## Roadmap

Coming soon...
