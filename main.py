#!/usr/bin/env python3
"""
Main Entry Point for Job Portal Scraping
Combines data from both Jora and Seek portals into a single CSV file
"""

import pandas as pd
import os
import concurrent.futures
import threading
from src.jora_crawler import JoraCrawler
from src.seek_crawler import SeekCrawler


def scrape_portal(crawler, portal_name, max_pages):
    """Scrape a single portal and return results"""
    try:
        print(f"\n{'='*60}")
        print(f"STARTING {portal_name.upper()} SCRAPING")
        print(f"{'='*60}")
        jobs = crawler.scrape_jobs(max_pages=max_pages)
        if jobs:
            print(f"{portal_name} scraping completed successfully. Jobs collected: {len(jobs)}")
            return jobs
        else:
            print(f" {portal_name} scraping failed or returned no data")
            return []
    except Exception as e:
        print(f"Error during {portal_name} scraping: {e}")
        return []


def main():
    """Main function to run both crawlers and combine results"""
    print("Job Portal Scraper - Combined Edition")
    print("=" * 60)
    print("This will scrape both Jora and Seek portals for sponsorship available jobs")
    print("All data will be combined into a single job_lists.csv file")
    print("Both crawlers will run simultaneously for faster execution")
    print("=" * 60)
    
    # Initialize crawlers
    jora_crawler = JoraCrawler()
    seek_crawler = SeekCrawler()
    
    all_jobs_data = []
    
    # Run both crawlers concurrently
    print("\n" + "=" * 60)
    print("STARTING CONCURRENT SCRAPING")
    print("Both Jora and Seek crawlers will run simultaneously")
    print("=" * 60)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        # Submit both crawler tasks
        jora_future = executor.submit(scrape_portal, jora_crawler, "Jora", 1)
        seek_future = executor.submit(scrape_portal, seek_crawler, "Seek", 1)
        
        # Wait for both to complete and collect results
        for future in concurrent.futures.as_completed([jora_future, seek_future]):
            try:
                jobs = future.result()
                if jobs:
                    all_jobs_data.extend(jobs)
            except Exception as e:
                print(f"Thread execution error: {e}")
    
    # Combine and save data
    if all_jobs_data:
        print("\n" + "=" * 60)
        print("COMBINING AND SAVING DATA")
        print("=" * 60)
        
        # Create DataFrame
        df = pd.DataFrame(all_jobs_data)
        
        # Ensure all required columns exist
        required_columns = ['title', 'company', 'location', 'salary', 'description', 'job_url', 'source']
        for col in required_columns:
            if col not in df.columns:
                df[col] = 'N/A'
        
        # Reorder columns to put source first
        column_order = ['source'] + [col for col in df.columns if col != 'source']
        df = df[column_order]
        
        # Save to CSV
        output_filename = "job_lists.csv"
        df.to_csv(output_filename, index=False, encoding='utf-8')
        
        # Print summary
        print(f" Combined data saved to: {output_filename}")
        print(f" Total jobs collected: {len(all_jobs_data)}")
        print(f" File size: {os.path.getsize(output_filename) / 1024:.1f} KB")
        
        # Print breakdown by source
        source_counts = df['source'].value_counts()
        print("\nJobs by source:")
        for source, count in source_counts.items():
            print(f"  - {source}: {count} jobs")
        

        
        print("\n" + "=" * 60)
        print("SCRAPING COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        
    else:
        print("\n No job data was collected from either portal.")
        print("Please check the individual scraper outputs above for errors.")


if __name__ == "__main__":
    main()
