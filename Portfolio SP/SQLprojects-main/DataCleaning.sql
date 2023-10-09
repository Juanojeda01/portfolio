/* Proyecto de Limpieza de Datos
	 Cleaning Data
*/

Select *
From Proyect..NashvilleHousing
----------------
-- Estandarizar formato de fecha
-- Standardize Date Format

Select SaleDateconverted, CONVERT(Date, Saledate)
From Proyect..NashvilleHousing

Update NashvilleHousing
SET SaleDate = CONVERT(Date, Saledate)

Alter Table NashvilleHousing
Add SaleDateconverted Date;


Update NashvilleHousing
Set SaleDateconverted = CONVERT(Date, Saledate)

-----------------
-- LLenar Data de direccion de propiedad por ParcelID
-- Fill by ParcelID


Select *
From Proyect..NashvilleHousing
Where PropertyAddress is null
Order by ParcelID

Select a.ParcelID, a.PropertyAddress, b.ParcelID, b.PropertyAddress, ISNULL(a.PropertyAddress,b.PropertyAddress)
From Proyect..NashvilleHousing a
Join Proyect..NashvilleHousing b
	on a.ParcelID = b.ParcelID
	and a.[UniqueID ] <> b.[UniqueID ]
where a.PropertyAddress is null
-- Update
Update a
Set PropertyAddress = ISNULL(a.PropertyAddress,b.PropertyAddress)
From Proyect..NashvilleHousing a
Join Proyect..NashvilleHousing b
	on a.ParcelID = b.ParcelID
	and a.[UniqueID ] <> b.[UniqueID ]
----------------------

-- Romper Adress en columnas individuales (Addres, City, State)
-- Breaking PropertyAdress
Select PropertyAddress
from Proyect..NashvilleHousing

Select
SUBSTRING(PropertyAddress, 1, CHARINDEX(',', PropertyAddress)- 1) as Address
, SUBSTRING(PropertyAddress, CHARINDEX(',', PropertyAddress) + 1, LEN(PropertyAddress)) as Address
from Proyect..NashvilleHousing


Alter Table NashvilleHousing
Add PropertySplitAddress Nvarchar(255);


Update NashvilleHousing
Set PropertySplitAddress = SUBSTRING(PropertyAddress, 1, CHARINDEX(',', PropertyAddress)- 1)

Alter Table NashvilleHousing
Add PropertySplitCity Nvarchar(255);


Update NashvilleHousing
Set PropertySplitCity = SUBSTRING(PropertyAddress, CHARINDEX(',', PropertyAddress) + 1, LEN(PropertyAddress))

Select * 
From Proyect..NashvilleHousing


-- Separar OwnerAddress
-- Spliting OwnerAddress
-------------------------------------------------------------------------

Select
PARSENAME(REPLACE(OwnerAddress,',','.'),3)
,PARSENAME(REPLACE(OwnerAddress,',','.'),2)
,PARSENAME(REPLACE(OwnerAddress,',','.'),1)
From Proyect..NashvilleHousing

Alter Table NashvilleHousing
Add OwnerSplitAddress Nvarchar(255);


Update NashvilleHousing
Set OwnerSplitAddress = PARSENAME(REPLACE(OwnerAddress,',','.'),3)

Alter Table NashvilleHousing
Add OwnerSplitCity Nvarchar(255);


Update NashvilleHousing
Set OwnerSplitCity = PARSENAME(REPLACE(OwnerAddress,',','.'),2)

Alter Table NashvilleHousing
Add OwnerSplitState Nvarchar(255);


Update NashvilleHousing
Set OwnerSplitState = PARSENAME(REPLACE(OwnerAddress,',','.'),1)

-----------------------------------------------------------------------------
-- Cambiar Y y N por Yes y No en SoldAsVacant
-- Change Y y N by Yes and No en SoldAsVacant
Select Distinct(SoldAsVacant), COUNT(SoldAsVacant)
From Proyect..NashvilleHousing
Group by SoldAsVacant
order by 2


Select SoldAsVacant
,CASE   When SoldAsVacant = 'Y' THEN 'Yes'
		When SoldAsVacant = 'N' THEN 'No'
		Else SoldAsVacant
		END
From Proyect..NashvilleHousing

Update NashvilleHousing
SET SoldAsVacant = CASE When SoldAsVacant = 'Y' THEN 'Yes'
		When SoldAsVacant = 'N' THEN 'No'
		Else SoldAsVacant
		END
From Proyect..NashvilleHousing

----------------------------------------------------------------------
-- Remover duplicados
-- Remove Duplicates

With RowNumCTE AS( 
Select *,
	ROW_NUMBER() OVER (
	PARTITION BY ParcelID,
				PropertyAddress,
				SalePrice,
				SaleDate,
				LegalReference
				ORDER BY
					UniqueID
					) row_num
From Proyect..NashvilleHousing
)

DELETE 
from RowNumCTE
Where row_num > 1

----- Borrar columnas sin usar
----- Delete Unused columns

Alter Table Proyect..NashvilleHousing
DROP COLUMN OwnerAddress, TaxDistrict, PropertyAddress

Select *
From Proyect..NashvilleHousing

Alter Table Proyect..NashvilleHousing
DROP COLUMN Saledate
