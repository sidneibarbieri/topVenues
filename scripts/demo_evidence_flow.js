async (page) => {
  await page.getByTestId("stSidebar").getByText("Evidence", { exact: true }).click();
  await page.waitForTimeout(7000);
  await page.getByText("Inspect audit criteria and provenance", { exact: true }).click();
  await page.waitForTimeout(7000);
  await page.mouse.wheel(0, 520);
  await page.waitForTimeout(7000);
  await page
    .getByTestId("stSidebar")
    .getByText("Dataset lifecycle", { exact: true })
    .click();
  await page.waitForTimeout(7000);
  await page.mouse.wheel(0, 600);
  await page.waitForTimeout(7000);
}
