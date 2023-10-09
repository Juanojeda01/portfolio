/*
Covid 19 Exploracion de datos / Data Exploratory

*/

Select *
From Proyect..covid_deaths
Where continent is not null 
order by 3,4


--Seleccionar data 
--Selecting data
Select Location, date, total_cases, new_cases, total_deaths, population
From Proyect..covid_deaths
Where Continent is not null 
order by 1,2


-- Total Cases vs Total Deaths
Select Location, date, total_cases,total_deaths, (total_deaths/total_cases)*100 as DeathPercentage
From Proyect..covid_deaths
and continent is not null 
order by 1,2


-- Total Cases vs Population
Select Location, date, Population, total_cases,  (total_cases/population)*100 as PercentPopulationInfected
From Proyect..covid_deaths
order by 1,2


--Highest Infection Rate compared to Population
--Mayor infeccion con respecto a la poblacion
Select Location, Population, MAX(total_cases) as HighestInfectionCount,  Max((total_cases/population))*100 as PercentPopulationInfected
From Proyect..covid_deaths
--Where location like '%states%'
Group by Location, Population
order by PercentPopulationInfected desc


-- Countries with Highest Death Count per Population
-- Paises con mayor total de muertes por poblacion
Select Location, MAX(cast(Total_deaths as int)) as TotalDeathCount
From Proyect..covid_deaths
Where continent is not null 
Group by Location
order by TotalDeathCount desc


-- CONTINENTES
-- Continentes con mayor conteo de muertes por poblacion
--contintents with the highest death count per population

Select continent, MAX(cast(Total_deaths as int)) as TotalDeathCount
From Proyect..covid_deaths
Where continent is not null 
Group by continent
order by TotalDeathCount desc


-- CIFRAS GLOBALES
-- GLOBAL NUMBERS


Select SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))/SUM(New_Cases)*100 as DeathPercentage
From Proyect..covid_deaths
where continent is not null 
order by 1,2

-- Total Population vs Vaccinations
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
From Proyect..covid_deaths dea
Join Proyect..covid_vaccinations vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
order by 2,3


-- Uso de CTE 
-- CTE use

With PopvsVac (Continent, Location, Date, Population, New_Vaccinations, RollingPeopleVaccinated)
as
(
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From Proyect..covid_deaths dea
Join Proyect..covid_vaccinations vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 
)
Select *, (RollingPeopleVaccinated/Population)*100
From PopvsVac



-- Using Temp Table 
-- Uso de tabla temporal

DROP Table if exists #PercentPopulationVaccinated
Create Table #PercentPopulationVaccinated
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population numeric,
New_vaccinations numeric,
RollingPeopleVaccinated numeric
)

Insert into #PercentPopulationVaccinated
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated

From Proyect..covid_deaths dea
Join Proyect..covid_vaccinations vac
	On dea.location = vac.location
	and dea.date = vac.date

Select *, (RollingPeopleVaccinated/Population)*100
From #PercentPopulationVaccinated



-- Crear una vista para guardar la data
-- Creating View to store data for later visualizations

Create View PercentPopulationVaccinated as
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by dea.Location Order by dea.location, dea.Date) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From Proyect..covid_deaths dea
Join Proyect..covid_vaccinations vac
	On dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null 

