SETTING UP YOUR LOCAL ENVIRONMENT
1. Install the [Astro CLI](https://docs.astronomer.io/astro/cli/overview)
2. Clone this repository to your machine
3. Make a copy of Dockerfile_Example and name it `Dockerfile`
4. Fill out the Dockerfile with your AWS credentials. You can set up AWS resources according to the requirements in documentation/process.md, or jonathanleek can provide you credentials to his environment if you are local.
5. From the commandline, initialize this directory using `astro dev init`

From this point, you can bring up  your local Airflow environment using `astro dev start` while you are developing, and use `astro dev stop` to stop the environment when not in use.